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
  ): NodeStateAtTime =

    var properties: PropertiesAtTime = Map.empty
    var edges: EdgesAtTime = Set.empty

    for
      eventsAtTime <- history if eventsAtTime.time <= time
      event <- eventsAtTime.events
    do
      event match
        case NodeRemoved =>
          properties = Map.empty
          edges = Set.empty

        case PropertyAdded(k, value) =>
          properties = properties + (k -> value)

        case PropertyRemoved(k) =>
          properties = properties - k

        case EdgeAdded(edge) =>
          edges = edges + edge

        case FarEdgeAdded(edge) =>
          edges = edges + edge

        case EdgeRemoved(edge) =>
          edges = edges - edge

        case FarEdgeRemoved(edge) =>
          edges = edges - edge

    NodeStateAtTime(
      time = history.lastOption.fold(StartOfTime)(_.time),
      sequence = history.lastOption.fold(0)(_.sequence),
      properties = properties,
      edges = edges
    )
