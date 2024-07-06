package andy42.graph.model

import org.msgpack.core.*
import zio.*

trait Edge extends Packable:
  val k: String
  val other: NodeId
  val direction: EdgeDirection

  def reverse(id: NodeId): Edge

  override def pack(using packer: MessagePacker): Unit =
    packer
      .packString(k)
      .addPayload(other.toArray) // Written without a header to save two bytes per NodeId
      .packInt(direction.ordinal)

final case class EdgeImpl(k: String, other: NodeId, direction: EdgeDirection) extends Edge with Packable:

  override def reverse(id: NodeId): Edge = EdgeImpl(k, id, direction.reversed)

object Edge extends Unpackable[Edge]:

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    Edge.unpack(isFar = false)

  def apply(k: String, other: NodeId, direction: EdgeDirection): Edge =
    EdgeImpl(k, other, direction)

  def unpack(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    for
      k <- UnpackSafely { unpacker.unpackString() }
      // The other NodeId is read/written without a length header to save two bytes
      otherIdBytes <- UnpackSafely { unpacker.readPayload(NodeId.byteLength) }
      other = NodeId(otherIdBytes)
      directionOrdinal <- UnpackSafely { unpacker.unpackInt() }
      direction <- ZIO
        .attempt(EdgeDirection.fromOrdinal(directionOrdinal))
        .orElseFail(UnexpectedEventDiscriminator(directionOrdinal))
    yield if isFar then EdgeImpl(k, other, direction) else EdgeImpl(k, other, direction)

enum EdgeDirection:
  case Outgoing extends EdgeDirection
  case Incoming extends EdgeDirection
  case Undirected extends EdgeDirection

  def reversed: EdgeDirection = this match
    case Incoming   => Outgoing
    case Outgoing   => Incoming
    case Undirected => Undirected

type EdgeSnapshot = Vector[Edge] // Collapsed properties at some point in time

object EdgeSnapshot:
  val empty: EdgeSnapshot = Vector.empty[Edge]
