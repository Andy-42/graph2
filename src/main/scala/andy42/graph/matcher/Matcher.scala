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

//  private val combination: Vector[Vector[_]] => Vector[Vector[_]] =
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
      unresolvedMatches: Vector[SpecNodeMatch]
  ): Option[Vector[SpecNodeMatch]] =
    @tailrec def accumulate(i: Int = 0, r: Vector[SpecNodeMatch] = Vector.empty): Option[Vector[SpecNodeMatch]] =
      if i >= unresolvedMatches.length then Some(r)
      else
        val current = unresolvedMatches(i)

        resolvedMatches.get(current._1) match
          case Some(matchedId) =>
            if current._2 != matchedId then None // Failed match - ids in the two matches conflict
            else accumulate(i + 1, r) // already matched
          case None =>
            accumulate(i + 1, r :+ current)
    accumulate()

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
    * @param unresolvedMatches
    *   The spec-node pairs that must be proven on this step for the resolvedMatches to be true.
    * @return
    *   All the possible resolved matches. More than one possible solution may be possible if an edge spec matches more
    *   than one edge.
    */
  def prove(
      resolvedMatches: ResolvedMatches,
      unresolvedMatches: Vector[SpecNodeMatch]
  ): NodeIO[Vector[ResolvedMatches]] =
    tracing.span("Matcher.prove") {
      for
        context <- tracing.getCurrentContext
        _ <- tracing.setAttribute("resolvedMatches", resolvedMatches.toSeq.map((spec, id) => s"$spec -> $id"))
        _ <- tracing.setAttribute("unresolvedMatches", unresolvedMatches.map((spec, id) => s"$spec -> $id"))

        snapshots <- fetchSnapshots(unresolvedMatches.map(_._2), context)

        (fullyResolved, partiallyResolved) = nextRoundOfProofs(resolvedMatches, unresolvedMatches, snapshots)

        _ <- tracing.setAttribute("fullyResolved", fullyResolved.length)
        _ <- tracing.setAttribute("partiallyResolved", partiallyResolved.length)

        withDependenciesResolved <- resolvePotentialMatchesInParallel {
          partiallyResolved.map { (resolvedMatches, unresolvedMatches) =>
            inTraceContext(context)(prove(resolvedMatches, unresolvedMatches))
          }
        }
      yield fullyResolved ++ withDependenciesResolved
    }

  def nextRoundOfProofs(
      resolvedMatches: ResolvedMatches,
      unresolvedMatches: Vector[(NodeSpecName, NodeId)],
      snapshots: Vector[NodeSnapshot]
  ): (Vector[ResolvedMatches], Vector[(ResolvedMatches, Vector[SpecNodeMatch])]) =

    def combineMatches(
        dependencyMatches: Vector[Vector[SpecNodeMatch]] // dependency / spec-node matches
    ): Option[(ResolvedMatches, Vector[SpecNodeMatch])] =
      require(dependencyMatches.length == unresolvedMatches.length)
      @tailrec def accumulate(
          i: Int = 0,
          nextResolvedMatches: ResolvedMatches = resolvedMatches,
          nextDependencies: Vector[SpecNodeMatch] = Vector.empty
      ): Option[(ResolvedMatches, Vector[SpecNodeMatch])] =
        if i >= unresolvedMatches.length then Some(nextResolvedMatches -> nextDependencies)
        else
          filterResolution(nextResolvedMatches, dependencyMatches(i)) match
            case None => None
            case Some(specNodeMatch) =>
              accumulate(
                i = i + 1,
                nextResolvedMatches = nextResolvedMatches + unresolvedMatches(i),
                nextDependencies ++ specNodeMatch
              )
      accumulate()

    def x =
      CombinationGenerator.doNotReuseElements(
        unresolvedMatches
          .zip(snapshots)
          .map { (unresolvedMatch, snapshot) =>
            val nodeSpec = subgraphSpec.nameToNodeSpec(unresolvedMatch._1)
            // combo / unresolved matches
            shallowMatch(nodeSpec, snapshot, resolvedMatches)
          }
      )

    val (fullyResolved, partiallyResolved) = x
      .flatMap(combineMatches)
      .partition(_._2.isEmpty)

    (fullyResolved.map(_._1).distinct, partiallyResolved)

  def shallowMatch(
      nodeSpec: NodeSpec,
      snapshot: NodeSnapshot,
      resolvedMatches: ResolvedMatches
  ): Vector[Vector[SpecNodeMatch]] = // combination / edges that must be proven for this combination to succeed

    val edgesThatAreNotAlreadyBoundToSomethingElse: Vector[Edge] =
      snapshot.edges.filter(edge => resolvedMatches.values.exists(_ == edge.other))

    def edgeBindsToId(id: NodeId): Vector[Edge] = snapshot.edges.filter(edge => edge.other == id)

    /** Match the edges in the snapshot to an EdgeSpec.
      *
      * If the nodeSpec that is the target of this edgeSpec is already bound, then the only possible match is that
      * already-bound node.
      *
      * Otherwise, we can only bind the node to nodes that have not already been resolved, because it is not allowed to
      * bind the same nodeSpec to more than one node.
      */
    def shallowMatchEdges(edgeSpec: EdgeSpec): Vector[SpecNodeMatch] =
      val toName = edgeSpec.direction.to.name

      resolvedMatches
        .get(toName)
        .fold(edgesThatAreNotAlreadyBoundToSomethingElse)(edgeBindsToId)
        .collect {
          case edge if edgeSpec.isShallowMatch(edge)(using snapshot) => toName -> edge.other
        }

    if !nodeSpec.allNodePredicatesMatch(using snapshot) then Vector.empty
    else
      // edgeSpec / all the nodeSpec-node matches that must be proven for this this node to match
      val possibleMatchesForEachEdgeSpec: Vector[Vector[SpecNodeMatch]] =
        subgraphSpec.outgoingEdges(nodeSpec.name).map(shallowMatchEdges)

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
