package andy42.graph.model

import scala.annotation.tailrec
import scala.collection.mutable

object CollapseNodeHistory:

  /** Collapse a Node's history to the state of the properties and edges at a given time.
    *
    * The events are ordered in ascending order of (time, sequence). Elements of the each EventsAtTime in the history
    * are included (in order) if their time is <= the given time.
    *
    * @param history
    *   A Node's complete history.
    * @param time
    *   The point in time to observe the node's state.
    */
  def apply(
      history: NodeHistory,
      time: EventTime = EndOfTime,
      previousSnapshot: Option[NodeSnapshot] = None
  ): NodeSnapshot =

    var properties: PropertySnapshot = previousSnapshot.fold(PropertySnapshot.empty)(_.properties)
    var edges: EdgeSnapshot = previousSnapshot.fold(EdgeSnapshot.empty)(_.edges)

    for
      eventsAtTime <- history if eventsAtTime.time <= time
      event <- eventsAtTime.events
    do
      event match
        case Event.NodeRemoved =>
          properties = Map.empty
          edges = Vector.empty

        case Event.PropertyAdded(k, value) =>
          properties = properties + (k -> value)

        case Event.PropertyRemoved(k) =>
          properties = properties - k

        case Event.EdgeAdded(edge) =>
          edges = if edges.contains(edge) then edges else edges :+ edge

        case Event.FarEdgeAdded(edge) =>
          edges = if edges.contains(edge) then edges else edges :+ edge

        case Event.EdgeRemoved(edge) =>
          edges = if edges.contains(edge) then edges.filter(_ != edge) else edges

        case Event.FarEdgeRemoved(edge) =>
          edges = if edges.contains(edge) then edges.filter(_ != edge) else edges

    NodeSnapshot(
      time = history.lastOption.fold(StartOfTime)(_.time),
      sequence = history.lastOption.fold(0)(_.sequence),
      properties = properties,
      edges = edges
    )
