package andy42.graph.matcher

import andy42.graph.config.{AppConfig, MatcherConfig}
import andy42.graph.matcher.CombinationGenerator.combinationsSingleUse
import andy42.graph.model.*
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.{Graph, TracingService}
import io.opentelemetry.context.Context
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

import scala.annotation.tailrec
import scala.collection.mutable

type NodeSpecName = String
type SpecNodeBinding = (NodeSpecName, NodeId)
type SpecNodeBindings = Map[NodeSpecName, NodeId]

object ResolvedBindings:
  val empty: Vector[SpecNodeBindings] = Vector.empty

trait Matcher:

  def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[SpecNodeBindings]]

final case class MatcherLive(
    config: MatcherConfig,
    subgraphSpec: SubgraphSpec,
    nodeSnapshotCache: MatcherSnapshotCache,
    tracing: Tracing,
    contextStorage: ContextStorage
) extends Matcher:

  /** Execute a ZIO effect within a given tracing context. The ZIO open telemetry context is maintained for the current
    * fiber, but if we want to maintain the tracing context across parallel invocations where the effects are run on a
    * different fiber (ZIO.foreachPar, ZIO.mergeAllPar), then we have to explicitly propagate the context.
    */
  private def inTraceContext[A](context: Context)(zio: => NodeIO[A])(implicit trace: Trace): NodeIO[A] =
    ZIO.acquireReleaseWith(contextStorage.getAndSet(context))(contextStorage.set(_))(_ => zio)

  /** Evaluate match resolution proofs in parallel, flattening the results. */
  private def resolveBindingsInParallel(
      proofs: Vector[NodeIO[Vector[SpecNodeBindings]]]
  ): NodeIO[Vector[SpecNodeBindings]] =
    ZIO.mergeAllPar(proofs)(ResolvedBindings.empty)(_ ++ _)

  private def fetchSnapshots(ids: Vector[NodeId], currentContext: Context): NodeIO[Vector[NodeSnapshot]] =
    ZIO.foreachPar(ids) { id => inTraceContext(currentContext)(nodeSnapshotCache.get(id)) }

  /** Match the given nodes using each node spec in the subgraph spec as a starting point.
    * @param ids
    *   The ids of the nodes to be matched.
    * @return
    *   The successfully resolved matches between the given nodes and the nodes in the spec. This result may contain
    *   duplicate matches.
    */
  override def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[SpecNodeBindings]] =
    tracing.root("Matcher.matchNodes") {
      for
        currentContext <- tracing.getCurrentContext
        _ <- tracing.setAttribute("nodeSpecs", subgraphSpec.allUniqueNodeSpecs.map(_.name))
        _ <- tracing.setAttribute("ids", ids.map(_.toString))

        snapshots <- fetchSnapshots(ids, currentContext)

        agenda = initialRoundOfProofs(ids, snapshots)

        alreadyResolved = agenda collect {
          case (resolvedBindings, unresolvedBindings) if unresolvedBindings.isEmpty => resolvedBindings
        }

        resolvedRecursively <- resolveBindingsInParallel {
          agenda collect {
            case (resolvedBindings, unresolvedBindings) if unresolvedBindings.nonEmpty =>
              inTraceContext(currentContext) {
                prove(resolvedBindings, unresolvedBindings)
              }
          }
        }
      yield (alreadyResolved ++ resolvedRecursively).distinct
    }

  def initialRoundOfProofs(
      ids: Vector[NodeId],
      snapshots: Vector[NodeSnapshot]
  ): Vector[(SpecNodeBindings, Vector[SpecNodeBinding])] =
    (
      for
        idAndSnapshot <- ids.zip(snapshots)
        (id, snapshot) = idAndSnapshot
        nodeSpec <- subgraphSpec.allUniqueNodeSpecs
        if !nodeSpec.isUnconstrained

        nextResolvedMatches = Map(nodeSpec.name -> id)
        edgeMatchesToProve <- shallowMatch(nodeSpec, snapshot, nextResolvedMatches)
      yield filterResolution(nextResolvedMatches, edgeMatchesToProve).map(nextResolvedMatches -> _)
    ).flatten

  def filterResolution(
      resolvedBindings: SpecNodeBindings,
      unresolvedBindings: Vector[SpecNodeBinding]
  ): Option[Vector[SpecNodeBinding]] =
    @tailrec def accumulate(
        i: Int = 0,
        r: Vector[SpecNodeBinding] = Vector.empty
    ): Option[Vector[SpecNodeBinding]] =
      if i >= unresolvedBindings.length then Some(r)
      else
        val current = unresolvedBindings(i)
        val (currentName, currentId) = current

        resolvedBindings.get(currentName) match
          case Some(matchedId) =>
            if currentId != matchedId then None // Failed match - ids in the two matches conflict
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
    * @param resolvedBindings
    *   The set of all spec-node pairs that have been resolve (or presumed resolved) at this point.
    * @param unresolvedBindings
    *   The spec-node pairs that must be proven on this step for the resolvedMatches to be true.
    * @return
    *   All the possible resolved matches. More than one possible solution may be possible if an edge spec matches more
    *   than one edge.
    */
  def prove(
      resolvedBindings: SpecNodeBindings,
      unresolvedBindings: Vector[SpecNodeBinding]
  ): NodeIO[Vector[SpecNodeBindings]] =
    tracing.span("Matcher.prove") {
      for
        context <- tracing.getCurrentContext
        _ <- tracing.setAttribute("resolvedMatches", resolvedBindings.toSeq.map((spec, id) => s"$spec -> $id"))
        _ <- tracing.setAttribute("unresolvedMatches", unresolvedBindings.map((spec, id) => s"$spec -> $id"))

        snapshots <- fetchSnapshots(unresolvedBindings.map(_._2), context)

        nextRound = nextRoundOfProofs(resolvedBindings, unresolvedBindings, snapshots)

        fullyResolved = nextRound collect {
          case (resolvedBindings, unresolvedBindings) if unresolvedBindings.isEmpty => resolvedBindings
        }

        partiallyResolved = nextRound collect {
          case (resolvedBindings, unresolvedBindings) if unresolvedBindings.nonEmpty =>
            inTraceContext(context)(prove(resolvedBindings, unresolvedBindings))
        }

        _ <- tracing.setAttribute("fullyResolved", fullyResolved.length)
        _ <- tracing.setAttribute("partiallyResolved", partiallyResolved.length)

        withDependenciesResolved <- resolveBindingsInParallel(partiallyResolved)
      yield fullyResolved ++ withDependenciesResolved
    }

  def nextRoundOfProofs(
      resolvedBindings: SpecNodeBindings,
      unresolvedBindings: Vector[(NodeSpecName, NodeId)],
      snapshots: Vector[NodeSnapshot]
  ): Vector[(SpecNodeBindings, Vector[SpecNodeBinding])] =

    def combine(
        bindingsForEachOfTheUnresolvedBindings: Vector[Vector[SpecNodeBinding]]
    ): Option[(SpecNodeBindings, Vector[SpecNodeBinding])] =
      require(bindingsForEachOfTheUnresolvedBindings.length == unresolvedBindings.length)

      @tailrec def accumulate(
          i: Int = 0,
          nextResolvedBindings: SpecNodeBindings = resolvedBindings,
          nextUnresolvedBindings: Vector[SpecNodeBinding] = Vector.empty
      ): Option[(SpecNodeBindings, Vector[SpecNodeBinding])] =
        if i >= bindingsForEachOfTheUnresolvedBindings.length then Some(nextResolvedBindings -> nextUnresolvedBindings)
        else
          filterResolution(nextResolvedBindings, bindingsForEachOfTheUnresolvedBindings(i)) match
            case None => None
            case Some(bindings) =>
              accumulate(
                i = i + 1,
                nextResolvedBindings = nextResolvedBindings + unresolvedBindings(i),
                nextUnresolvedBindings ++ bindings
              )
      accumulate()

    combinationsSingleUse(
      unresolvedBindings
        .zip(snapshots)
        .map { case ((nodeSpecName, _), snapshot) =>
          shallowMatch(
            nodeSpec = subgraphSpec.nameToNodeSpec(nodeSpecName),
            snapshot = snapshot,
            resolvedBindings = resolvedBindings
          )
        }
    )
      .flatMap(combine)

  def shallowMatch(
      nodeSpec: NodeSpec,
      snapshot: NodeSnapshot,
      resolvedBindings: SpecNodeBindings
  ): Vector[Vector[SpecNodeBinding]] =

    // Will be used
    lazy val edgesThatAreNotAlreadyBoundToSomethingElse: Vector[Edge] =
      snapshot.edges.filter { edge =>
        !resolvedBindings.values.exists(_ == edge.other)
      }

    def edgeBindsToId(id: NodeId): Vector[Edge] = snapshot.edges.filter(_.other == id)

    /** Match the edges in the snapshot to an EdgeSpec.
      *
      * If the nodeSpec that is the target of this edgeSpec is already bound, then the only possible match is that
      * already-bound node.
      *
      * Otherwise, we can only bind the node to nodes that have not already been resolved, because it is not allowed to
      * bind the same nodeSpec to more than one node.
      */
    def shallowMatchEdges(edgeSpec: EdgeSpec): Vector[SpecNodeBinding] =
      val targetNodeSpecName = edgeSpec.direction.to.name

      resolvedBindings
        .get(targetNodeSpecName)
        .fold(edgesThatAreNotAlreadyBoundToSomethingElse)(edgeBindsToId)
        .collect {
          case edge if edgeSpec.isShallowMatch(edge)(using snapshot) => targetNodeSpecName -> edge.other
        }

    if !nodeSpec.allNodePredicatesMatch(using snapshot) then Vector.empty
    else
      combinationsSingleUse(
        subgraphSpec.outgoingEdges(nodeSpec.name).map(shallowMatchEdges)
      )

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
