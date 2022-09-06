package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackToVector
import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import zio.IO
import zio.ZIO
import java.io.IOException

object EventHistory extends Unpackable[Vector[EventsAtTime]] {

  val empty: Vector[EventsAtTime] = Vector.empty

  override def unpack(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, Vector[EventsAtTime]] = {
    for {
      a <- unpackToVector(EventsAtTime.unpack, unpacker.hasNext)
    } yield a
  }.refineOrDie(UnpackFailure.refine)

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  def pack(
      eventHistory: Vector[EventsAtTime]
  )(implicit packer: MessagePacker): MessagePacker = {
    eventHistory.foreach(_.pack)
    packer
  }

  def packToArray(eventHistory: Vector[EventsAtTime]): PackedNodeContents = {
    val packer = MessagePack.newDefaultBufferPacker()
    pack(eventHistory)(packer)
    packer.toByteArray()
  }

  /**
    * Append an EventsAtTime at the end of history.
    * This can be more efficient since it doesn't require unpacking all of history.
    * The caller must ensure that eventsAtTime occurs after the last event (wrt. atTime, sequence)
    * as though the eventsAtTime were unpacked.
    *
    * @param packed The packed history for a node.
    * @param eventsAtTime Events to be appended to the end of the node's history.
    * @return The packed history, with the new EventsAtTime packed and appended to the history.
    */
  def append(packed: Array[Byte], eventsAtTime: EventsAtTime): Array[Byte] = {
    implicit val packer = MessagePack.newDefaultBufferPacker()
    eventsAtTime.pack
    packed ++ packer.toByteArray()
  }
}
