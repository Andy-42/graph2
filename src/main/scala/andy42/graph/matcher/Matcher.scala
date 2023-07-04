package andy42.graph.matcher

import andy42.graph.config.{AppConfig, MatcherConfig}
import andy42.graph.model.*
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.{Graph, TracingService}
import io.opentelemetry.context.Context
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import scala.collection.mutable

import scala.annotation.tailrec

type NodeSpecName = String
type SpecNodeMatch = (NodeSpecName, NodeId)
type ResolvedMatches = Map[NodeSpecName, NodeId]

object ResolvedMatches:
  val empty: ResolvedMatches = Map.empty

trait Matcher:

  def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[ResolvedMatches]]

final case class MatcherLive(
    config: MatcherConfig,
    subgraphSpec: SubgraphSpec,
    nodeSnapshotCache: MatcherSnapshotCache,
    tracing: Tracing,
    contextStorage: ContextStorage
) extends Matcher:

  /* The input to these combination generators will be a vector containing one element for each edge spec,
   * for a node that is being matched to some node spec.
   * Each element of that vector enumerates all the edges on that node that shallow match the corresponding
   * edge spec. An edge can match multiple edge specs, so there can be multiple combinations can be generated
   * that produce a valid match
   */
  type PossibleMatchesForEdgeSpec = Vector[SpecNodeMatch]
  type PossibleMatchesForThisNode = Vector[SpecNodeMatch]

//  //                     edgeSpec               combo
//  private val combination: Vector[Vector[T]] => Vector[Vector[T]] =
//    config.combinationGenerator match
//      case "all"                   => CombinationGenerator.all
//      case "do-not-reuse-elements" => CombinationGenerator.doNotReuseElements

  /** Execute a ZIO effect within a given tracing context. The ZIO open telemetry context is maintained for the current
    * fiber, but if we want to maintain the tracing context across parallel invocations where the effects are run on a
    * different fiber (ZIO.foreachPar, ZIO.mergeAllPar), then we have to explicitly propagate the context.
    */
  private def inTraceContext[A](context: Context)(zio: => NodeIO[A])(implicit trace: Trace): NodeIO[A] =
    ZIO.acquireReleaseWith(contextStorage.getAndSet(context))(contextStorage.set(_))(_ => zio)

  /** Evaluate match resolution proofs in parallel, flattening the results. */
  private def resolvePotentialMatchesInParallel(
      proofs: Vector[NodeIO[Vector[ResolvedMatches]]]
  ): NodeIO[Vector[ResolvedMatches]] =
    ZIO.mergeAllPar(proofs)(Vector.empty[ResolvedMatches])(_ ++ _)

  private def fetchSnapshots(ids: Vector[NodeId], currentContext: Context): NodeIO[Vector[NodeSnapshot]] =
    ZIO.foreachPar(ids) { id => inTraceContext(currentContext)(nodeSnapshotCache.get(id)) }

  /** Match the given nodes using each node spec in the subgraph spec as a starting point.
    * @param ids
    *   The ids of the nodes to be matched.
    * @return
    *   The successfully resolved matches between the given nodes and the nodes in the spec. This result may contain
    *   duplicate matches.
    */
  override def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[ResolvedMatches]] =
    tracing.root("Matcher.matchNodes") {
      for
        currentContext <- tracing.getCurrentContext
        _ <- tracing.setAttribute("nodeSpecs", subgraphSpec.allUniqueNodeSpecs.map(_.name))
        _ <- tracing.setAttribute("ids", ids.map(_.toString))

        snapshots <- fetchSnapshots(ids, currentContext)

        resolvedMatches <- resolvePotentialMatchesInParallel {
          initialRoundOfProofs(ids, snapshots, currentContext)
        }
      yield resolvedMatches.distinct
    }

  def initialRoundOfProofs(
      ids: Vector[NodeId],
      snapshots: Vector[NodeSnapshot],
      context: Context
  ): Vector[NodeIO[Vector[ResolvedMatches]]] =
    for
      idAndSnapshot <- ids.zip(snapshots)
      (id, snapshot) = idAndSnapshot
      nodeSpec <- subgraphSpec.allUniqueNodeSpecs
      if !nodeSpec.isUnconstrained

      nextResolvedMatches = Map(nodeSpec.name -> id)
      edgeMatchesToProve <- shallowMatch(nodeSpec, snapshot, nextResolvedMatches)
    yield filterResolution(nextResolvedMatches, edgeMatchesToProve) match
      case Some(Vector())              => ZIO.succeed(Vector(nextResolvedMatches)) // Match is proven
      case Some(unresolvedEdgeMatches) => inTraceContext(context)(prove(nextResolvedMatches, unresolvedEdgeMatches))
      case None                        => ZIO.succeed(Vector.empty) // Match is disproven

  def filterResolution(
      resolvedMatches: ResolvedMatches,
      possibleCombinationOfEdgeMatchesForOneNode: Vector[(NodeSpecName, NodeId)]
  ): Option[Vector[SpecNodeMatch]] =

    @tailrec
    def accumulate(i: Int = 0, r: Vector[SpecNodeMatch] = Vector.empty): Option[Vector[SpecNodeMatch]] =
      if i >= possibleCombinationOfEdgeMatchesForOneNode.length then Some(r)
      else
        val current = possibleCombinationOfEdgeMatchesForOneNode(i)

        resolvedMatches.get(current._1) match
          case Some(matchedId) =>
            if current._2 != matchedId then None // Failed match - ids in the two matches conflict
            else accumulate(i + 1, r) // already matched
          case None =>
            accumulate(i + 1, r :+ current)

    if possibleCombinationOfEdgeMatchesForOneNode.isEmpty then None
    else accumulate()

  /** Expand the proof for a node by traversing its unproven dependencies.
    *
    * The node-id keys matches are tested using shallow matching.
    *
    * Any failed matches have their dependencies removed from resolved matches. That dependency results in an invalid
    * dependency (i.e., one or more edges now has zero matches), then that entry is removed from resolved matches in the
    * next recursion. If resolved matches becomes empty, then the original spec-node being proven has failed to be
    * proven, so the overall match fails.
    *
    * If the match has not failed, then the proven spec-node pairs are added to resolvedMatches. This may add new
    * unproven matches that are proven on the next recursion.
    *
    * @param resolvedMatches
    *   The set of all spec-node pairs that have been resolve (or presumed resolved) at this point.
    * @param dependencies
    *   The spec-node pairs that must be proven on this step for the resolvedMatches to be true.
    * @return
    *   All the possible resolved matches. More than one possible solution may be possible if an edge spec matches more
    *   than one edge.
    */
  def prove(
      resolvedMatches: ResolvedMatches,
      dependencies: Vector[SpecNodeMatch]
  ): NodeIO[Vector[ResolvedMatches]] =
    tracing.span("Matcher.prove") {
      for
        context <- tracing.getCurrentContext
        _ <- tracing.setAttribute("resolvedMatches", resolvedMatches.toSeq.map((spec, id) => s"$spec -> $id"))
        _ <- tracing.setAttribute("dependencies", dependencies.map(_.toString))

        snapshots <- fetchSnapshots(dependencies.map(_._2), context)

        withDependenciesResolved <- resolvePotentialMatchesInParallel {
          nextRoundOfProofs(resolvedMatches, dependencies, snapshots, context)
        }
      yield withDependenciesResolved
    }

  def nextRoundOfProofs(
      resolvedMatches: ResolvedMatches,
      dependencies: Vector[SpecNodeMatch],
      snapshots: Vector[NodeSnapshot],
      context: Context
  ): Vector[NodeIO[Vector[ResolvedMatches]]] =
    //                         depend / combo/ matches to prove this combo
    val dependencyComboMatches: Vector[Vector[Vector[SpecNodeMatch]]] =
      for
        tuple <- dependencies.zip(snapshots)
        ((nodeSpecName, _), snapshot) = tuple
        nodeSpec = subgraphSpec.nameToNodeSpec(nodeSpecName)
      yield shallowMatch(nodeSpec, snapshot, resolvedMatches)

    // If any dependency has no viable combinations that match, then the overall match fails.
    if dependencyComboMatches.exists(_.isEmpty) then Vector(ZIO.succeed(Vector.empty)) // match fails
    else
      // for each combo, we create a result that is more or less a flattening of the inner two vectors,
      // but taking into account that combinations should be rejected if they have two matches to the
      // same nodeSpec but with different ids.

      // At each step, use filterResolution to combine the possible matches for that combination.

      // This flattens out the dependency level
      // The option represents that a match may fail

      def combineMatchesForOneCombination(
          comboDependencyMatches: Vector[Vector[SpecNodeMatch]]
      ): Option[(ResolvedMatches, Vector[SpecNodeMatch])] =
        require(comboDependencyMatches.length == dependencies.length)

        @tailrec
        def accumulate(
            i: Int = 0,
            nextResolvedMatches: ResolvedMatches = resolvedMatches,
            nextDependencies: Vector[SpecNodeMatch] = Vector.empty
        ): Option[(ResolvedMatches, Vector[SpecNodeMatch])] =
          if i >= dependencies.length then Some(nextResolvedMatches -> nextDependencies)
          else
            filterResolution(nextResolvedMatches, comboDependencyMatches(i)) match
              case None => None
              case Some(specNodeMatch) =>
                accumulate(
                  i = i + 1,
                  nextResolvedMatches = nextResolvedMatches + dependencies(i),
                  nextDependencies ++ specNodeMatch
                )

        accumulate()

      // Since each dependency might have more than one combination, combining those dependencies
      // requires applying the combination generator again.
      // dependency/combo/matches => combo/dependency/matches
      CombinationGenerator
        .doNotReuseElements[Vector[SpecNodeMatch]](dependencyComboMatches)
        // Combine all the dependencies for one combination into a single Vector[SpecNodeMatch]
        .map(combineMatchesForOneCombination)
        .map {
          case Some((resolvedMatches, Vector())) => // match is proven
            ZIO.succeed(Vector(resolvedMatches))
          case Some((resolvedMatches, unresolvedMatches)) => // more more dependencies to resolve
            inTraceContext(context)(prove(resolvedMatches, unresolvedMatches))
          case None => // match is disproven
            ZIO.succeed(Vector.empty)
        }

  def shallowMatch(
      nodeSpec: NodeSpec,
      snapshot: NodeSnapshot,
      resolvedMatches: ResolvedMatches
  ): Vector[PossibleMatchesForThisNode] = // combination / edges that must be proven for this combination to succeed

    /** Match the edges in the snapshot to an EdgeSpec. If target (other id) of the edgeSpec.direction.to.name already
      * appears in resolvedMatches, then the only edges that can be matched must have that other id. If the target edge
      * name does not appear in resolvedMatches yet, then all edges can be considered.
      */
    def shallowMatchEdges(edgeSpec: EdgeSpec): Vector[SpecNodeMatch] =
      resolvedMatches
        .get(edgeSpec.direction.to.name)
        .fold(snapshot.edges)(previouslyMatchedNode => snapshot.edges.filter(_.other == previouslyMatchedNode))
        .collect {
          case edge if edgeSpec.isShallowMatch(edge)(using snapshot) => edgeSpec.direction.to.name -> edge.other
        }

    if !nodeSpec.allNodePredicatesMatch(using snapshot) then Vector.empty
    else
      // edgeSpec / all the nodeSpec-node matches that must be proven for this this node to match
      val possibleMatchesForEachEdgeSpec: Vector[Vector[SpecNodeMatch]] =
        subgraphSpec.outgoingEdges(nodeSpec.name).map(shallowMatchEdges)

      if possibleMatchesForEachEdgeSpec.exists(_.isEmpty) then
        // All edges must have at least one possible match, or the match fails
        Vector.empty
      else
        // Combination: combo / all the spec-node matches needed to prove this combination
        CombinationGenerator.doNotReuseElements[SpecNodeMatch](possibleMatchesForEachEdgeSpec)

object Matcher:

  def make(
      config: MatcherConfig,
      time: EventTime,
      graph: Graph,
      tracing: Tracing,
      subgraphSpec: SubgraphSpec,
      nodes: Vector[Node],
      contextStorage: ContextStorage
  ): UIO[MatcherLive] =
    for
      nodeCache <- MatcherNodeCache.make(graph, nodes, tracing)
      dataViewCache <- MatcherSnapshotCache.make(time, nodeCache, tracing)
    yield MatcherLive(config, subgraphSpec, dataViewCache, tracing, contextStorage)
