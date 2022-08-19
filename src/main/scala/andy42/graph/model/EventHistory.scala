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
      length <- ZIO.attempt { unpacker.unpackInt() }
      a <- unpackToVector(EventsAtTime.unpack, length)
    } yield a
  }.refineOrDie(UnpackFailure.refine)

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  def pack(
      eventHistory: Vector[EventsAtTime]
  )(implicit packer: MessagePacker): MessagePacker = {
    packer.packInt(eventHistory.length)
    eventHistory.foreach(_.pack)
    packer
  }

  def packToArray(eventHistory: Vector[EventsAtTime]): PackedNodeContents = {
    val packer = MessagePack.newDefaultBufferPacker()
    pack(eventHistory)(packer)
    packer.toByteArray()
  }
}
