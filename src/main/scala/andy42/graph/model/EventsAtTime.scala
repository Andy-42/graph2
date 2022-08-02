package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackToVector
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import zio.Task
import zio.ZIO

/**
 * A group of events that changed the state of a node at a given time.
 * This corresponds to a single row that would be appended to the persistent store
 * as a result of ingesting mutation events.
 *
 * If a node is changed more than once for the same eventTime, the sequence number is incremented.
 * This is done since the persisted history of a node is only ever written by appending to the history.
 *
 * The events are a sequence of mutation events that happened within that eventTime and sequence.
 * This group of events will have had redundant events removed, but will preserve the order that
 * they were originally added to the graph.
 *
 * @param eventTime The time of the mutation event, in epoch millis.
 * @param sequence  The sequence number of the change within a single eventTime, starting at 0.
 * @param events    The mutation events at that time.
 */
case class EventsAtTime(eventTime: EventTime,
                        sequence: Int,
                        events: Vector[Event]) extends Packable {

  require(events.nonEmpty)
  require(sequence >= 0)

  override def pack(implicit packer: MessagePacker): MessagePacker = {
    packer
      .packLong(eventTime)
      .packInt(sequence)
      .packInt(events.length)

    events.foreach(_.pack)

    packer
  }
}

object EventsAtTime extends Unpackable[EventsAtTime] {

 override def unpack(implicit unpacker: MessageUnpacker): Task[EventsAtTime] =
    for {
      eventTime <- ZIO.attempt(unpacker.unpackLong())
      sequence <- ZIO.attempt(unpacker.unpackInt())
      length <- ZIO.attempt(unpacker.unpackInt())
      events <- unpackToVector(Event.unpack, length)
    } yield EventsAtTime(eventTime, sequence, events)

  /**
    * Packing an entire node's history into a single array of bytes is way to retain the
    * node history in memory while minimizing the number of objects in the heap (reduce GC time).
    */
  def pack(eventsAtTime: EventsAtTime)(implicit packer: MessagePacker): MessagePacker = {
    packer.packLong(eventsAtTime.eventTime)
    packer.packInt(eventsAtTime.sequence)
    eventsAtTime.events.foreach(_.pack)
    packer
  }
}
