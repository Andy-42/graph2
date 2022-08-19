package andy42.graph.model

import scala.annotation.tailrec
import scala.collection.mutable

object EventDeduplicationOps {

  // Mutation events are always ordered from newest to oldest

  /**
   * Remove any duplicate mutation events.
   * An event is a duplicate if a later element of `events` would cancel out this event.
   * This is not expected to be a common occurrence, but this is done to minimize the node's journal.
   * The implementation avoids copying (or any allocation) if there are no duplicates.
   */
  def deduplicateWithinEvents(events: Vector[Event]): Vector[Event] =
    if (!events.indices.exists(i => isDuplicatedLaterAt(events, i)))
      events
    else {
      val buffer = mutable.ArrayBuffer.empty[Event]
      buffer.sizeHint(events.length - 1)

      @tailrec
      def accumulate(i: Int = 0): Vector[Event] =
        if (i == events.length)
          buffer.toVector
        else {
          if (!isDuplicatedLaterAt(events, i))
            buffer += events(i)

          accumulate(i = i + 1)
        }

      accumulate()
    }

  def isDuplicatedLaterAt(events: Vector[Event], i: Int): Boolean = {

    val event = events(i)

    @tailrec
    def accumulate(j: Int = i + 1): Boolean =
      if (j == events.length)
        false
      else if (eventIsDuplicatedBy(event = event, laterEvent = events(j)))
        true
      else
        accumulate(j = j + 1)

    accumulate()
  }

  def isDuplicatePropertyEvent(k1: String, laterEvent: Event): Boolean =
    laterEvent match {
      case PropertyAdded(k2, _) => k1 == k2
      case PropertyRemoved(k2) => k1 == k2
      case _ => false
    }

  def isDuplicateEdgeEvent(edge1: Edge, laterEvent: Event): Boolean =
    laterEvent match {
      case EdgeAdded(edge2) => edge1 == edge2
      case EdgeRemoved(edge2) => edge1 == edge2
      case _ => false
    }

  def eventIsDuplicatedBy(event: Event, laterEvent: Event): Boolean =
    event match {
      case NodeRemoved => laterEvent == NodeRemoved

      case PropertyAdded(k, _) => isDuplicatePropertyEvent(k, laterEvent)
      case PropertyRemoved(k) => isDuplicatePropertyEvent(k, laterEvent)

      case EdgeAdded(edge) => isDuplicateEdgeEvent(edge, laterEvent)
      case EdgeRemoved(edge) => isDuplicateEdgeEvent(edge, laterEvent)
      case FarEdgeAdded(edge) => isDuplicateEdgeEvent(edge, laterEvent)
      case FarEdgeRemoved(edge) => isDuplicateEdgeEvent(edge, laterEvent)
    }
}
