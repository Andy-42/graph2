package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackCountedToSeq
import org.msgpack.core.*
import zio.*

import java.io.IOException

/** A group of events that changed the state of a node at a given time. This corresponds to a single row that would be
  * appended to the persistent store as a result of ingesting mutation events.
  *
  * If a node is changed more than once for the same time, the sequence number is incremented. This is done since the
  * persisted history of a node is only ever written by appending to the history.
  *
  * The events are a sequence of mutation events that happened within that time and sequence. This group of events will
  * have had redundant events removed, but will preserve the order that they were originally added to the graph.
  *
  * @param time
  *   The time of the mutation event, in epoch millis.
  * @param sequence
  *   The sequence number of the change within a single time, starting at 0.
  * @param events
  *   Events that change the node state.
  */
final case class EventsAtTime(
    time: EventTime,
    sequence: Int,
    events: Vector[Event]
) extends Packable:
  require(events.nonEmpty)
  require(sequence >= 0)

  override def pack(implicit packer: MessagePacker): Unit =
    packer
      .packLong(time)
      .packInt(sequence)
      .packInt(events.length)
    events.foreach(_.pack)

object EventsAtTime extends Unpackable[EventsAtTime]:

  val empty: EventsAtTime = EventsAtTime(StartOfTime, 0, Vector.empty)

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, EventsAtTime] =
    UnpackSafely {
      for
        time <- ZIO.attempt(unpacker.unpackLong())
        sequence <- ZIO.attempt(unpacker.unpackInt())
        length <- ZIO.attempt(unpacker.unpackInt())
        events <- unpackCountedToSeq(Event.unpack, length)
      yield EventsAtTime(time, sequence, events.toVector)
    }
