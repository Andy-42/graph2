package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackToVector
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import zio.Task
import zio.ZIO

object EventHistory {

  val empty: Vector[EventsAtTime] = Vector.empty
  
  def unpack(implicit unpacker: MessageUnpacker): Task[Vector[EventsAtTime]] =
    for {
      length <- ZIO.attempt(unpacker.unpackInt())
      a <- unpackToVector(EventsAtTime.unpack, length)
    } yield a.toVector

  def pack(eventHistory: Vector[EventsAtTime])(implicit packer: MessagePacker): MessagePacker = {
    packer.packInt(eventHistory.length)
    eventHistory.foreach(_.pack)
    packer
  }
}
