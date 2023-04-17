package andy42.graph.services

import andy42.graph.matcher.{Matcher, MatchingNodes, SubgraphSpec}
import andy42.graph.model.*
import zio.*

case class SubgraphMatch(time: EventTime, subgraphSpec: SubgraphSpec, nodeMatches: List[MatchingNodes])

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
  ): NodeIO[Unit]
  
  def output: Hub[SubgraphMatch]

// TODO: s/b able to have multiple standing queries?

final case class StandingQueryEvaluationLive(graph: Graph, subgraphSpec: SubgraphSpec, hub: Hub[SubgraphMatch])
    extends StandingQueryEvaluation:

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
  ): NodeIO[Unit] =
    for
      changedNodes <- allAffectedNodes(time, mutations)
      _ <- matchAgainstStandingQueries(time, changedNodes)
    yield ()

  override def output: Hub[SubgraphMatch] = hub

  /** Get the new state for all nodes that were modified in this group of changes. The Graph will pass the new node
    * states after the changes have been applied, but since it will not include any far edge events (since that is done
    * in an eventually-consistent way), so we retrieve any nodes affected by edge events and patch them as though the
    * far edges had been modified.
    */
  private def allAffectedNodes(
      time: EventTime,
      mutations: Vector[NodeMutationOutput]
  ): NodeIO[Vector[Node]] =

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

    def appendFarEdgeEvents(node: Node): NodeIO[Node] =
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
  ): NodeIO[Unit] =
    for
      matcher <- Matcher.make(time, graph, subgraphSpec, changedNodes)
      nodeMatches <- matcher.matchNodes(changedNodes.map(_.id))
      _ <- if nodeMatches.nonEmpty then hub.offer(SubgraphMatch(time, subgraphSpec, nodeMatches)) else ZIO.unit
    yield ZIO.unit

object StandingQueryEvaluation:
  def make(subgraphSpec: SubgraphSpec): URIO[Graph, StandingQueryEvaluation] =
    for
      graph <- ZIO.service[Graph]
      hub <- Hub.unbounded[SubgraphMatch]
    yield StandingQueryEvaluationLive(graph, subgraphSpec, hub)
