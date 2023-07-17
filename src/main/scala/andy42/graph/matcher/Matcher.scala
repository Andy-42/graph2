package andy42.graph.matcher

import andy42.graph.config.MatcherConfig
import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

type NodeSpecName = String
type SpecNodeBinding = (NodeSpecName, NodeId)
type SpecNodeBindings = Map[NodeSpecName, NodeId]

trait Matcher:

  def matchNodesToSubgraphSpec(nodes: Seq[Node]): NodeIO[Seq[SpecNodeBindings]]

final case class MatcherLive(
    config: MatcherConfig,
    subgraphSpec: SubgraphSpec,
    snapshotCache: MatcherSnapshotCache,
    tracing: Tracing,
    contextStorage: ContextStorage
) extends Matcher:

  import BindingInProgress.*
  import CombinationGenerator.*

  extension (binding: SpecNodeBinding)

    /** Shallow match a `NodeSpec` to a `Node`.
      *
      * The `binding` represents a possible match between a `NodeSpec` and a `Node`. A shallow match evaluates whether
      * the node predicates hold, and if so, the edge predicates are evaluated against each of the `Node`'s edges.
      *
      * @return
      *   An input for a combination generator for NodeSpec-NodeId bindings. The outer vector corresponds to each edge
      *   in the spec, and the inner vector is all the matches for that edge for that node. The outer vector can be
      *   empty if the node predicate or any of the edge predicates fail.
      */
    def shallowMatch: NodeIO[GeneratorInput[SpecNodeBinding]] =
      val (nodeSpecName, id) = binding
      val nodeSpec = subgraphSpec.nameToNodeSpec(nodeSpecName)
      val edgeSpecs = subgraphSpec.outgoingEdges(nodeSpecName)
      require(edgeSpecs.nonEmpty)

      for given NodeSnapshot <- snapshotCache.get(id)
      yield
        if !nodeSpec.allNodePredicatesMatch then EmptyCombinationInput.shallowMatch
        else
          optimizeIfEmpty(EmptyCombinationInput.shallowMatch) {
            edgeSpecs.foldLeft(EmptyCombinationInput.shallowMatch) {
              case (s, _) if s.nonEmpty && s.last.isEmpty => s

              case (s, edgeSpec) =>
                s :+ summon[NodeSnapshot].edges.collect {
                  case edge if edgeSpec.isShallowMatch(edge) => nodeSpec.name -> edge.other
                }
            }
          }

    def possibleBindings: NodeIO[Seq[BindingInProgress]] =
      for shallowMatches <- shallowMatch
      yield generateCombinations(shallowMatches) // combinations that use exactly one of each of the edge matches
        .flatMap { unresolved =>
          val resolved = Map(binding)
          combineUnresolved(resolved, unresolved)
            .map(BindingInProgress(resolved, _))
        }

  def allNodeSpecMutationPairs(mutations: Seq[NodeId]): Seq[SpecNodeBinding] =
    for
      nodeSpec <- subgraphSpec.allUniqueNodeSpecs
      mutation <- mutations
    yield nodeSpec.name -> mutation

  /** Calculate all match starting points when only looking at individual matches.
    *
    * A Matcher would be configured this way when:
    *   - A scan of the repository, so there are no other mutations to consider, or
    *   - The data being ingested is not known to be a connected set of nodes.
    *
    * This path can always be used when there is only a single mutation.
    *
    * This case only has a single level of combination generation.
    *
    * @param mutations
    *   The group of nodes to be matched against.
    * @return
    */
  def initialMatchIndividual(mutations: Seq[NodeId]): NodeIO[Seq[BindingInProgress]] =
    ZIO.foldLeft(allNodeSpecMutationPairs(mutations))(Vector.empty[BindingInProgress]) { (s, binding) =>
      binding.possibleBindings.map(s ++ _)
    }

  def optimizeIfEmpty[T](optimizedEmpty: GeneratorInput[T])(a: GeneratorInput[T]): GeneratorInput[T] =
    if a.nonEmpty && a.last.isEmpty then optimizedEmpty else a

  def initialMatchesWhereAllNodesMustMatch(
      mutations: Seq[NodeId]
  ): NodeIO[Seq[BindingInProgress]] =

    def resolvedBindingExistsForEveryMutation(a: BindingInProgress): Boolean =
      mutations.forall(id => a.resolved.exists(_._2 == id))

    for possibleBindingsForEachNodeSpec <-
        ZIO
          .foldLeft(subgraphSpec.allUniqueNodeSpecs)(EmptyCombinationInput.bindingInProgress) {
            // If some nodeSpec failed to have any matches then there is no need to evaluate any more nodeSpecs
            case (s1, _) if s1.nonEmpty && s1.last.isEmpty => ZIO.succeed(s1)

            case (s1, nodeSpec) =>
              ZIO
                .foldLeft(mutations)(Vector.empty[BindingInProgress]) { (s2, id) =>
                  (nodeSpec.name, id).possibleBindings.map(s2 ++ _)
                }
                .map(s1 :+ _)
          }
    yield generateCombinations(possibleBindingsForEachNodeSpec)
      .flatMap(BindingInProgress.combineBindingsInProgress)
      .filter(resolvedBindingExistsForEveryMutation)

  /** Match the given nodes using each node spec in the subgraph spec as a starting point.
    * @param nodes
    *   The ids of the nodes to be matched.
    * @return
    *   The successfully resolved matches between the given nodes and the nodes in the spec. This result may contain
    *   duplicate matches.
    */
  override def matchNodesToSubgraphSpec(nodes: Seq[Node]): NodeIO[Seq[SpecNodeBindings]] =
  
    val mutations = nodes.map(_.id)
    tracing.root("Matcher.matchNodesInMutationGroupToSubgraphSpec2") {
      for
        _ <- tracing.setAttribute("nodeSpecs", subgraphSpec.allUniqueNodeSpecs.map(_.name))
        _ <- tracing.setAttribute("mutations", mutations.map(_.toString))

        initialMatch <-
          if config.allNodesInMutationGroupMustMatch && mutations.length > 1 then
            initialMatchesWhereAllNodesMustMatch(mutations)
          else initialMatchIndividual(mutations)

        fullyResolved = initialMatch collect {
          case bindingInProgress if bindingInProgress.isFullyResolved => bindingInProgress.resolved
        }

        partiallyResolved = initialMatch collect {
          case bindingInProgress if !bindingInProgress.isFullyResolved => bindingInProgress
        }

        // Ensure that all snapshots that we will need for the next wave of proofs are in the cache or being fetched.
        // The asynchronous pre-fetching is the major element of parallelism in this matcher.
        _ <- snapshotCache.prefetch(partiallyResolved.flatMap(_.unresolved.values))

        resolvedRecursively <- ZIO.foldLeft(partiallyResolved)(Vector.empty[SpecNodeBindings]) {
          (s, bindingInProgress) => prove(bindingInProgress).map(s ++ _)
        }
      yield fullyResolved ++ resolvedRecursively
    }

  /** Expand the proof for a node by traversing its unproven dependencies.
    *
    * The node-id keys matches are tested using shallow matching.
    *
    * Any failed matches have their dependencies removed from resolved matches. That dependency results in an invalid
    * dependency (i.e., one or more edges now has zero matches), then that entry is removed from resolved matches in the
    * next recursion. If resolved matches becomes empty, then the original spec-node being proven has failed to be
    * proven, so the overall match fails.
    *
    * If the match has not failed, then the proven spec-node pairs are added to resolvedBindings. This may add new
    * unproven matches that are proven on the next recursion.
    *
    * @return
    *   All the possible resolved matches. More than one possible solution may be possible if an edge spec matches more
    *   than one edge.
    */
  private def prove(bindingInProgress: BindingInProgress): NodeIO[Seq[SpecNodeBindings]] =
    tracing.span("Matcher.prove") {
      for
        context <- tracing.getCurrentContextUnsafe
        _ <- tracing.setAttribute(
          "resolvedBindings",
          bindingInProgress.resolved.map((spec, id) => s"$spec -> $id").mkString("\n")
        )
        _ <- tracing.setAttribute(
          "unresolvedBindings",
          bindingInProgress.unresolved.map((spec, id) => s"$spec -> $id").mkString("\n")
        )

        nextRound <- nextRoundOfProofs(bindingInProgress)

        fullyResolved = nextRound collect {
          case binding if binding.isFullyResolved => binding.resolved
        }

        partiallyResolved = nextRound.filter(_.isPartiallyResolved)

        // Start fetching all the nodes that we are going to need up front
        _ <- snapshotCache.prefetch(partiallyResolved.flatMap(_.unresolved.values))

        resolvedRecursively <- ZIO.foldLeft(partiallyResolved)(Vector.empty[SpecNodeBindings]) { (s, binding) =>
          prove(binding).map(s ++ _)
        }
      yield fullyResolved ++ resolvedRecursively
    }

  def nextRoundOfProofs(bindingInProgress: BindingInProgress): NodeIO[Seq[BindingInProgress]] =
    for bindingsToProveUnresolvedBindings <-
        ZIO
          .foldLeft(bindingInProgress.unresolved)(EmptyCombinationInput.bindingInProgress) {
            case (s, _) if s.nonEmpty && s.last.isEmpty => ZIO.succeed(s)
            case (s, binding)                           => binding.possibleBindings.map(s :+ _)
          }
          .map(optimizeIfEmpty(EmptyCombinationInput.bindingInProgress))
    yield generateCombinations(bindingsToProveUnresolvedBindings)
      .flatMap(BindingInProgress.combineBindingsInProgress)

object EmptyCombinationInput:

  import CombinationGenerator.*

  val shallowMatch: GeneratorInput[SpecNodeBinding] = emptyInput[SpecNodeBinding]
  val bindingInProgress: GeneratorInput[BindingInProgress] = emptyInput[BindingInProgress]

object Matcher:

  def make(
      config: MatcherConfig,
      time: EventTime,
      graph: Graph,
      subgraphSpec: SubgraphSpec,
      affectedNodes: Vector[Node],
      tracing: Tracing,
      contextStorage: ContextStorage
  ): IO[NodeIOFailure, MatcherLive] =
    for snapshotCache <- MatcherSnapshotCache.make(time, graph, affectedNodes, tracing)
    yield MatcherLive(config, subgraphSpec, snapshotCache, tracing, contextStorage)
