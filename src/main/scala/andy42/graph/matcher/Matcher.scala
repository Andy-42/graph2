package andy42.graph.matcher

import andy42.graph.config.{AppConfig, MatcherConfig}
import andy42.graph.model.*
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.{Graph, TracingService}
import io.opentelemetry.context.Context
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

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
   *
   * The output of the combination generator is
   */

  type PossibleMatchesForEdgeSpec = Vector[SpecNodeMatch]
  type PossibleMatchesToProveForThisNode = Vector[SpecNodeMatch]

  private val combinationGenerator: Vector[PossibleMatchesForEdgeSpec] => Vector[PossibleMatchesToProveForThisNode] =
    config.combinationGenerator match {
      case "all"                   => CombinationGenerator.all[SpecNodeMatch]
      case "do-not-reuse-elements" => CombinationGenerator.doNotReuseElements[SpecNodeMatch]
    }

  import tracing.aspects.{root, span}

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

      possibleMatchForThisNode <- shallowMatch(nodeSpec, Map.empty, snapshot)

      nextResolvedMatches = Map(nodeSpec.name -> id)
      unresolved = possibleMatchForThisNode.filter { case (nodeSpecName, _) => nodeSpecName != nodeSpec.name }
    yield
      if unresolved.isEmpty then ZIO.succeed(Vector(nextResolvedMatches))
      else inTraceContext(context)(prove(nextResolvedMatches, unresolved))

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
    for
      tuple <- dependencies.zip(snapshots)
      ((nodeSpecName, id), snapshot) = tuple
      nodeSpec = subgraphSpec.nameToNodeSpec(nodeSpecName)

      possibleMatches <- shallowMatch(nodeSpec, resolvedMatches, snapshot)

      nextResolvedMatches = resolvedMatches.updated(nodeSpecName, id)
      unresolved = possibleMatches.filter { case (nodeSpecName, _) =>
        !nextResolvedMatches.contains(nodeSpecName)
      }
    yield
      if unresolved.isEmpty then ZIO.succeed(Vector(nextResolvedMatches))
      else inTraceContext(context)(prove(nextResolvedMatches, unresolved))

  def shallowMatch(
      nodeSpec: NodeSpec,
      resolvedMatches: ResolvedMatches,
      snapshot: NodeSnapshot
  ): Vector[PossibleMatchesToProveForThisNode] =

    /** Match the edges in the snapshot to an EdgeSpec. If target (other id) of the edgeSpec.direction.to.name already
      * appears in resolvedMatches, then the only edges that can be matched must have that other id. If the target does
      * not appear in resolvedMatches yet, then all edges can be considered.
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
      val x = subgraphSpec
        .outgoingEdges(nodeSpec.name)
        .map(shallowMatchEdges)

      if x.exists(_.isEmpty) then Vector.empty
      else combinationGenerator(x)

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
