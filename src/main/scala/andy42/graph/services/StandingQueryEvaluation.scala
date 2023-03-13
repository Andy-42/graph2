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
    * Since far edges might not be synced yet, when we pull in a node for matching, we can patch the far edges up to
    * that they are consistent.
    */
  def graphChanged(
      time: EventTime,
      changes: Vector[NodeMutationOutput]
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
      mutations: Vector[NodeMutationOutput]
  ): IO[UnpackFailure | PersistenceFailure, Unit] =
    // TODO: Config, no match on only far edge events.

    for
      changedNodes <- allAffectedNodes(time, mutations)
      _ <- matchAgainstStandingQueries(time, changedNodes)
    yield ()

  /**
    * Get the new state for all nodes that were modified in this group of changes.
    * The Graph will pass the new node states after the changes have been applied, but since
    * it will not include any far edge events (since that is done in an eventually-consistent way),
    * so we retrieve any nodes affected by edge events and patch them as though the far edges had been
    * modified.
    */
  private def allAffectedNodes(
      time: EventTime,
      mutations: Vector[NodeMutationOutput]
  ): IO[UnpackFailure | PersistenceFailure, Vector[Node]] =

    def farEdgeEvents: Vector[(NodeId, Event)] =
      for
        mutation <- mutations
        NodeMutationOutput(node, events) = mutation
        referencedNodeAndFarEdgeEvent <- events.collect {
          case event: Event.EdgeAdded   => event.edge.other -> Event.FarEdgeAdded(event.edge.reverse(node.id))
          case event: Event.EdgeRemoved => event.edge.other -> Event.FarEdgeRemoved(event.edge.reverse(node.id))
        }
      yield referencedNodeAndFarEdgeEvent

    val farEdgeEventsGroupedById: Map[NodeId, Vector[Event]] =
      farEdgeEvents
        .groupBy((id, _) => id)
        .map((k, v) => k -> v.map(_._2))

    // Nodes referenced in NearEdge Events that are not already part of this mutation
    val additionalReferencedNodeIds = farEdgeEventsGroupedById.keys.filter { id =>
      !mutations.exists(_.node.id == id)
    }

    def appendFarEdgeEvents(node: Node): IO[UnpackFailure | PersistenceFailure, Node] =
      farEdgeEventsGroupedById
        .get(node.id)
        .fold(ZIO.succeed(node))(events => node.append(time, events))

    for
      additionalReferencedNodes <- ZIO.foreachPar(additionalReferencedNodeIds)(graph.get)
      updatedNodes <- ZIO.foreach(mutations.map(_.node) ++ additionalReferencedNodes)(appendFarEdgeEvents)
    yield updatedNodes

  private def matchAgainstStandingQueries(
    time: EventTime,
    changedNodes: Vector[Node]
  ): IO[UnpackFailure | PersistenceFailure, Unit] = ???
  // for each node in changedNodes and each standing query, match the subgraph against the standing query
  // The keys in the initial changedNode are the ones that are matched.
  // As matching proceeds, accumulate all the nodes visited in a cache

object StandingQueryEvaluation:
  val layer: URLayer[Graph, StandingQueryEvaluation] =
    ZLayer {
      for graph <- ZIO.service[Graph]
        // TODO: Service that sinks the standing query output stream
      yield StandingQueryEvaluationLive(graph)
    }
