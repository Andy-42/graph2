package andy42.graph.model

import java.util.UUID

/** A NodeId identifies a node in the graph. This is a UUID-like representation, and it is stored as two longs to avoid
  * allocating another object (i.e., a byte array).
  */
final case class NodeId(msb: Long, lsb: Long) extends Ordered[NodeId]:

  override def toString: String = new UUID(msb, lsb).toString

  // Same hash function as for UUID
  override def hashCode: Int =
    val x = msb ^ lsb
    (x >> 32).toInt ^ x.toInt

  def toArray: Array[Byte] =
    val r = Array.ofDim[Byte](NodeId.byteLength)
    r(0) = ((msb >> 56) & 0xff).toByte
    r(1) = ((msb >> 48) & 0xff).toByte
    r(2) = ((msb >> 40) & 0xff).toByte
    r(3) = ((msb >> 32) & 0xff).toByte
    r(4) = ((msb >> 24) & 0xff).toByte
    r(5) = ((msb >> 16) & 0xff).toByte
    r(6) = ((msb >> 8) & 0xff).toByte
    r(7) = ((msb >> 0) & 0xff).toByte
    r(8) = ((lsb >> 56) & 0xff).toByte
    r(9) = ((lsb >> 48) & 0xff).toByte
    r(10) = ((lsb >> 40) & 0xff).toByte
    r(11) = ((lsb >> 32) & 0xff).toByte
    r(12) = ((lsb >> 24) & 0xff).toByte
    r(13) = ((lsb >> 16) & 0xff).toByte
    r(14) = ((lsb >> 8) & 0xff).toByte
    r(15) = ((lsb >> 0) & 0xff).toByte
    r

  override def compare(that: NodeId): Int =
    if msb < that.msb then -1
    else if msb > that.msb then 1
    else if lsb < that.lsb then -1
    else if lsb > that.lsb then 1
    else 0

object NodeId:
  
  val byteLength: Int = 16
  
  def apply(id: Array[Byte]): NodeId =
    require(id.length == NodeId.byteLength)

    val msb =
      ((id(0).toLong & 0xff) << 56) |
        ((id(1).toLong & 0xff) << 48) |
        ((id(2).toLong & 0xff) << 40) |
        ((id(3).toLong & 0xff) << 32) |
        ((id(4).toLong & 0xff) << 24) |
        ((id(5).toLong & 0xff) << 16) |
        ((id(6).toLong & 0xff) << 8) |
        ((id(7).toLong & 0xff) << 0)

    val lsb =
      ((id(0 + 8).toLong & 0xff) << 56) |
        ((id(1 + 8).toLong & 0xff) << 48) |
        ((id(2 + 8).toLong & 0xff) << 40) |
        ((id(3 + 8).toLong & 0xff) << 32) |
        ((id(4 + 8).toLong & 0xff) << 24) |
        ((id(5 + 8).toLong & 0xff) << 16) |
        ((id(6 + 8).toLong & 0xff) << 8) |
        ((id(7 + 8).toLong & 0xff) << 0)

    NodeId(msb, lsb)

  def apply(id: Vector[Byte]): NodeId = NodeId(id.toArray)

  def apply(id: UUID): NodeId =
    NodeId(msb = id.getMostSignificantBits, lsb = id.getLeastSignificantBits)
