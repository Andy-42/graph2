package andy42.graph.services

import andy42.graph.model.{Event, EventTime, EventsAtTime, Node, NodeId, UnpackFailure}
import zio.*
import zio.stm.STM
import zio.stm.TQueue

/** Observe a node that is changed.
  */
trait StandingQueryEvaluation:

  /** A node has changed. The implementation will attempt to match all standing queries using this node as a starting
    * point.
    *
    * This node may have additional history appended to it that is not yet persisted.
    *
    * Since far edges might not be synced yet, when we pull in a node for matching, we can patch the far edges up to
    * that they are consistent.
    */
  def graphChanged(
      time: EventTime,
      changes: Vector[GroupedGraphMutationOutput]
  ): IO[UnpackFailure | PersistenceFailure, Unit]

  // TODO: How to get the output stream?

final case class StandingQueryEvaluationLive(graph: Graph) extends StandingQueryEvaluation:

  /** Match the node change against all standing queries.
    *
    * Events are only appended to one node at one time, but it may include edge events that have only been applied to
    * the near node and are not yet reflected in the Graph. We expand the set of affected nodes so that these other
    * nodes appear with their far edge events added. This allows matching to be done with a consistent image of the
    * changes.
    *
    * We may choose to not match against standing queries if the newEvents are purely far edge events. This creates a
    * possibility of missing a match, so this behaviour is configurable.
    */
  override def graphChanged(
      time: EventTime,
      mutations: Vector[GroupedGraphMutationOutput]
  ): IO[UnpackFailure | PersistenceFailure, Unit] =
    // TODO: Config, no match on only far edge events.

    for
      changedNodes: Map[NodeId, Node] <- allAffectedNodes(time, mutations)
      _ <- matchAgainstStandingQueries(changedNodes, time)
    yield ()

  private def allAffectedNodes(
      time: EventTime,
      mutations: Vector[GroupedGraphMutationOutput]
  ): IO[UnpackFailure | PersistenceFailure, Map[NodeId, Node]] =

    def farEdgeEvents: Vector[(NodeId, Event)] =
      for
        mutation <- mutations
        id = mutation.node.id
        referencedNodeAndFarEdgeEvent: (NodeId, Event) <- mutation.events.collect {
          case event: Event.EdgeAdded   => event.edge.other -> Event.FarEdgeAdded(event.edge.reverse(id))
          case event: Event.EdgeRemoved => event.edge.other -> Event.FarEdgeRemoved(event.edge.reverse(id))
        }
      yield referencedNodeAndFarEdgeEvent

    type NodeMap = Map[NodeId, Node]
    type EdgeEventMap = Map[NodeId, Vector[Event]]

    val farEdgeEventsGroupedById: EdgeEventMap =
      farEdgeEvents
        .groupBy((id, _) => id)
        .map((k, v) => k -> v.map(_._2).toVector)

    val nodesInMutation: NodeMap = mutations.map(mutation => mutation.node.id -> mutation.node).toMap

    // Nodes referenced in NearEdge Events that are not already part of this mutation
    val additionalReferencedNodeIds = farEdgeEventsGroupedById.keys.filter(!nodesInMutation.contains(_))

    def updateFarEdgeEvents(node: Node, events: Vector[Event]): IO[UnpackFailure, Node] =
      for
        x <- node.append(time, events)
        (node, _) = x
      yield node

    for
      additionalReferencedNodes <- ZIO.foreachPar(additionalReferencedNodeIds) { graph.get }
      additionalReferencedNodesMap: NodeMap = additionalReferencedNodes.map(node => node.id -> node).toMap
      xxx: NodeMap = nodesInMutation ++ additionalReferencedNodesMap
    yield xxx
      .map((id: NodeId, node: Node) =>

        farEdgeEventsGroupedById
          .get(id)
          .fold(node)(updateFarEdgeEvents(node, _))

        ???
      )

  private def matchAgainstStandingQueries(
      changedNodes: Map[NodeId, Node],
      time: EventTime
  ): IO[UnpackFailure | PersistenceFailure, Unit] = ???
  // for each node in changedNode and each standing query, match the subgraph against the standing query
  // The keys in the initial changedNode are the ones that are matched.
  // As matching proceeds, accumulate all the nodes visited in a cache

object StandingQueryEvaluation:
  val layer: URLayer[Graph, StandingQueryEvaluation] =
    ZLayer {
      for graph <- ZIO.service[Graph]
        // TODO: Service that sinks the standing query output stream
      yield StandingQueryEvaluationLive(graph)
    }
