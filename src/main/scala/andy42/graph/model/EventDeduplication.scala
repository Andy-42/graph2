package andy42.graph.model

import scala.annotation.tailrec
import scala.collection.mutable

object EventDeduplication:

  // Mutation events are always ordered from newest to oldest

  /** Remove any duplicate mutation events.
    *
    * An event is a duplicate if a later element of `events` would cancel out this event. This is not expected to be a
    * common occurrence, but this is done to minimize the node's history. The implementation avoids copying (or any
    * allocation) if there are no duplicates.
    */
  def deduplicateWithinEvents(events: Vector[Event]): Vector[Event] =
    if !events.indices.exists(i => isDuplicatedLaterAt(events, i)) then events
    else
      events.zipWithIndex.collect {
        case (event, i) if !isDuplicatedLaterAt(events, i) => event
      }

  def isDuplicatedLaterAt(events: Vector[Event], i: Int): Boolean =

    val event = events(i)

    @tailrec
    def accumulate(j: Int = i + 1): Boolean =
      if j == events.length then false
      else if eventIsDuplicatedBy(event = event, laterEvent = events(j)) then true
      else accumulate(j = j + 1)

    accumulate()

  def isDuplicatePropertyEvent(k1: String, laterEvent: Event): Boolean =
    laterEvent match
      case Event.PropertyAdded(k2, _) => k1 == k2
      case Event.PropertyRemoved(k2)  => k1 == k2
      case _                          => false

  def isDuplicateEdgeEvent(edge1: Edge, laterEvent: Event): Boolean =
    laterEvent match
      case Event.EdgeAdded(edge2)   => edge1 == edge2
      case Event.EdgeRemoved(edge2) => edge1 == edge2
      case _                        => false

  def eventIsDuplicatedBy(event: Event, laterEvent: Event): Boolean =
    event match
      case Event.NodeRemoved          => laterEvent == Event.NodeRemoved
      case Event.PropertyAdded(k, _)  => isDuplicatePropertyEvent(k, laterEvent)
      case Event.PropertyRemoved(k)   => isDuplicatePropertyEvent(k, laterEvent)
      case Event.EdgeAdded(edge)      => isDuplicateEdgeEvent(edge, laterEvent)
      case Event.EdgeRemoved(edge)    => isDuplicateEdgeEvent(edge, laterEvent)
      case Event.FarEdgeAdded(edge)   => isDuplicateEdgeEvent(edge, laterEvent)
      case Event.FarEdgeRemoved(edge) => isDuplicateEdgeEvent(edge, laterEvent)
