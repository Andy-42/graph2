package andy42.graph.model

import org.msgpack.core._
import zio._

import scala.util.hashing.MurmurHash3


final case class Edge(k: String, other: NodeId, direction: EdgeDirection) extends Packable:

  def reverse(id: NodeId): Edge = this.direction match
    case EdgeDirection.Outgoing   => copy(other = id, direction = EdgeDirection.Incoming)
    case EdgeDirection.Incoming   => copy(other = id, direction = EdgeDirection.Outgoing)
    case EdgeDirection.Undirected => copy(other = id, direction = EdgeDirection.Undirected)

  /** A hash of the edge that can be used to test eventual consistency of the far edge state.
    *
    * forall(x => x ^ x == 0)
    *
    * This works even with a larger aggregation there are many values being included in no particular order. As long as
    * each item in is ^-ed into the aggregate twice. Each edge's hash will include the the key, and the ids at either
    * end once.
    *
    * forall(edge => (edge.hash(id) ^ edge.reverse(id).hash(edge.other) == 0)
    *
    * @param id
    *   The id of the node containing this half-edge.
    * @return
    *   A hash of the edge that will reconcile to zero when ^-ed with the hash of the edge reversed.
    */
  def hash(id: NodeId): EdgeHash =
    MurmurHash3.stringHash(k) ^
      MurmurHash3.arrayHash(id.toArray) ^
      MurmurHash3.arrayHash(other.toArray) ^
      direction.hashContribution

  override def pack(using packer: MessagePacker): Unit =
    packer
      .packString(k)
      .packBinaryHeader(other.length)
      .writePayload(other.to(Array))
      .packInt(direction.ordinal)

object Edge extends Unpackable[Edge]:
  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] =
    UnpackSafely {
      for
        k <- ZIO.attempt(unpacker.unpackString())
        length <- ZIO.attempt(unpacker.unpackBinaryHeader())
        other <- ZIO.attempt(unpacker.readPayload(length).toVector)
        directionOrdinal <- ZIO.attempt(unpacker.unpackInt())
        direction <- ZIO
          .attempt(EdgeDirection.fromOrdinal(directionOrdinal))
          .mapError(_ => UnexpectedEventDiscriminator(directionOrdinal))
      yield Edge(k, other, direction)
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
