package andy42.graph.model

import scala.annotation.tailrec
import scala.collection.mutable

object CollapseNodeHistory {

  /** Collapse a sequence of events to a current state. The events are ordered
    * in ascending order of event time.
    *
    * The PropertyAdded events represents values as org.msgpack.value.Value,
    * which are converted to their graph representation in properties.
    *
    * @param events
    *   Events ordered from oldest to newest.
    * @return
    *   The properties and edge states representing the current state after all
    *   event are applied.
    */
  def apply(
      history: Vector[EventsAtTime],
      atTime: EventTime = EndOfTime,
      initialProperties: PropertiesAtTime = Map.empty,
      initialEdges: EdgesAtTime = Set.empty
  ): NodeStateAtTime = {

    var properties: PropertiesAtTime = initialProperties
    var edges: EdgesAtTime = initialEdges

    for {
      eventsAtTime <- history
      if eventsAtTime.eventTime <= atTime
      event <- eventsAtTime.events
    } event match {
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
    }

    NodeStateAtTime(
      eventTime = history.lastOption.fold(StartOfTime)(_.eventTime),
      sequence = history.lastOption.fold(0)(_.sequence),
      properties = properties,
      edges = edges,
    )
  }
}
