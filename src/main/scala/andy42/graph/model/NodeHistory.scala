package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackUncountedToSeq
import org.msgpack.core.*
import zio.*

type NodeHistory = Vector[EventsAtTime]

extension (nodeHistory: NodeHistory)

  def pack(using packer: MessagePacker): Unit =
    nodeHistory.foreach(_.pack)

  def toPacked: Array[Byte] =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    pack
    packer.toByteArray

object NodeHistory extends Unpackable[NodeHistory]:

  val empty: NodeHistory = Vector.empty

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, NodeHistory] =
    for a <- unpackUncountedToSeq(EventsAtTime.unpack, unpacker.hasNext)
    yield a.toVector

  def unpackNodeHistory(packed: PackedNodeHistory): IO[UnpackFailure, NodeHistory] =
    NodeHistory.unpack(using MessagePack.newDefaultUnpacker(packed))
