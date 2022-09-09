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
      time: EventTime = EndOfTime
  ): NodeSnapshot =

    var properties: PropertySnapshot = PropertySnapshot.empty
    var edges: EdgeSnapshot = EdgeSnapshot.empty

    for
      eventsAtTime <- history if eventsAtTime.time <= time
      event <- eventsAtTime.events
    do
      event match
        case Event.NodeRemoved =>
          properties = Map.empty
          edges = Set.empty

        case Event.PropertyAdded(k, value) =>
          properties = properties + (k -> value)

        case Event.PropertyRemoved(k) =>
          properties = properties - k

        case Event.EdgeAdded(edge) =>
          edges = edges + edge

        case Event.FarEdgeAdded(edge) =>
          edges = edges + edge

        case Event.EdgeRemoved(edge) =>
          edges = edges - edge

        case Event.FarEdgeRemoved(edge) =>
          edges = edges - edge

    NodeSnapshot(
      time = history.lastOption.fold(StartOfTime)(_.time),
      sequence = history.lastOption.fold(0)(_.sequence),
      properties = properties,
      edges = edges
    )
