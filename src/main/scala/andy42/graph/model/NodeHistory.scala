package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackToVector
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import zio.IO
import zio.ZIO

import java.io.IOException

type NodeHistory = Vector[EventsAtTime]

extension (nodeHistory: NodeHistory)
  def toByteArray: Array[Byte] =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    NodeHistory.pack(nodeHistory)
    packer.toByteArray

object NodeHistory extends Unpackable[NodeHistory]:

  val empty: NodeHistory = Vector.empty

  def unpack(packed: PackedNodeHistory): IO[UnpackFailure, NodeHistory] =
    unpack(using MessagePack.newDefaultUnpacker(packed))

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, NodeHistory] = {
    for a <- unpackToVector(EventsAtTime.unpack, unpacker.hasNext)
    yield a
  }.refineOrDie(UnpackFailure.refine)

  // TODO:
  def unpackNodeHistory(packed: PackedNodeHistory): IO[UnpackFailure, NodeHistory] =
    NodeHistory.unpack(using MessagePack.newDefaultUnpacker(packed))

  def pack(eventHistory: NodeHistory)(using packer: MessagePacker): MessagePacker =
    eventHistory.foreach(_.pack)
    packer

  def packToArray(eventHistory: NodeHistory): PackedNodeHistory =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    pack(eventHistory)
    packer.toByteArray()

  /** Append an EventsAtTime at the end of history. This can be more efficient since it doesn't require unpacking all of
    * history. The caller must ensure that eventsAtTime occurs after the last event (wrt. time, sequence) as though
    * the eventsAtTime were unpacked.
    *
    * @param packed
    *   The packed history for a node.
    * @param eventsAtTime
    *   Events to be appended to the end of the node's history.
    * @return
    *   The packed history, with the new EventsAtTime packed and appended to the history.
    */
  def append(packed: PackedNodeHistory, eventsAtTime: EventsAtTime): Array[Byte] =
    implicit val packer = MessagePack.newDefaultBufferPacker()
    eventsAtTime.pack
    packed ++ packer.toByteArray()
