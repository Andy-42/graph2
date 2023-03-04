package andy42.graph.model

import org.msgpack.core.*
import zio.*

import scala.util.hashing.MurmurHash3

trait Edge extends Packable:
  val k: String
  val other: NodeId
  val direction: EdgeDirection

  def isNear: Boolean
  def isFar: Boolean = !isNear

  /** A hash of the edge that can be used to test eventual consistency of the far edge state.
    *
    * {{{
    * forall(x => x ^ x == 0)
    * }}}
    *
    * This works even with a larger aggregation there are many values being included in no particular order. As long as
    * each item in is XOR-ed into the aggregate twice. Each edge's hash will include the the key, and the ids at either
    * end once.
    *
    * {{{
    * forall(edge => (edge.hash(id) ^ edge.reverse(id).hash(edge.other) == 0)
    * }}}
    *
    * @param id
    *   The id of the node containing this half-edge.
    * @return
    *   A hash of the edge that will reconcile to zero when XOR-ed with the hash of the edge reversed.
    */
  def hash(id: NodeId): EdgeHash =
    MurmurHash3.stringHash(k) ^
      id.hashCode ^
      other.hashCode ^
      direction.hashContribution

  def reverse(id: NodeId): Edge

  override def pack(using packer: MessagePacker): Unit =
    packer
      .packString(k)
      .packBinaryHeader(other.id.length)
      .writePayload(other.id)
      .packInt(direction.ordinal)

final case class NearEdge(k: String, other: NodeId, direction: EdgeDirection) extends Edge with Packable:

  override def reverse(id: NodeId): Edge = FarEdge(k, id, direction.reversed)

  override def isNear: Boolean = true

final case class FarEdge(k: String, other: NodeId, direction: EdgeDirection) extends Edge with Packable:

  override def reverse(id: NodeId): Edge = NearEdge(k, id, direction.reversed)

  override def isNear: Boolean = true

object NearEdge extends Unpackable[Edge]:

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    Edge.unpack(isFar = true)

object FarEdge extends Unpackable[Edge]:

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    Edge.unpack(isFar = false)

object Edge:

  def unpack(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    UnpackSafely {
      for
        k <- ZIO.attempt(unpacker.unpackString())
        length <- ZIO.attempt(unpacker.unpackBinaryHeader())
        other <- ZIO.attempt(NodeId(unpacker.readPayload(length).toArray))
        directionOrdinal <- ZIO.attempt(unpacker.unpackInt())
        direction <- ZIO
          .attempt(EdgeDirection.fromOrdinal(directionOrdinal))
          .mapError(_ => UnexpectedEventDiscriminator(directionOrdinal))
      yield if isFar then FarEdge(k, other, direction) else NearEdge(k, other, direction)
    }

type EdgeHash = Long // Correctly balanced edges will reconcile to zero

enum EdgeDirection:
  case Outgoing extends EdgeDirection
  case Incoming extends EdgeDirection
  case Undirected extends EdgeDirection

  def hashContribution: Long = this match
    case Incoming | Outgoing => 0 // Symmetric so that reversing produces the same hash
    case Undirected          => 1

  def reversed: EdgeDirection = this match
    case Incoming   => Outgoing
    case Outgoing   => Incoming
    case Undirected => Undirected

type EdgeSnapshot = Set[Edge] // Collapsed properties at some point in time

object EdgeSnapshot:
  val empty = Set.empty[Edge]
