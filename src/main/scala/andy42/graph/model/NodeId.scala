package andy42.graph.model

import java.util.UUID

/**
  * A NodeId identifies a node in the graph.
  * This is a UUID-like representation, and it is stored as two longs to avoid allocating another object (i.e., a byte array).
  */
final case class NodeId(msb: Long, lsb: Long) extends Ordered[NodeId]:

  override def toString: String = new UUID(msb, lsb).toString

  // Same hash function as for UUID
  override def hashCode: Int =
    val x = msb ^ lsb
    (x >> 32).toInt ^ x.toInt

  def toArray: Array[Byte] =
    val r = Array.ofDim[Byte](16)

    inline def extractTo(v: Long, offset: Int): Unit =
      var i = 0
      while i < 8 do
        r(i + offset) = ((v >> (i * 8)) & 0xff).toByte
        i += 1

      extractTo(msb, 0)
      extractTo(lsb, 8)

    r

  override def compare(that: NodeId): Int =
    if msb < that.msb then -1
    else if msb > that.msb then 1
    else if lsb < that.lsb then -1
    else if lsb > that.lsb then 1
    else 0

object NodeId:
  def apply(id: Array[Byte]): NodeId =
    require(id.length == 16)

    inline def extractLong(start: Int, limit: Int): Long =
      var r = 0L
      var i = start
      while i < limit do
        r = (r << 8) | (id(i).toLong & 0xff)
        i += 1

      r

    NodeId(msb = extractLong(0, 8), lsb = extractLong(8, 16))

  def apply(id: Vector[Byte]): NodeId = NodeId(id.toArray)

  def apply(id: UUID): NodeId =
    NodeId(msb = id.getMostSignificantBits, lsb = id.getLeastSignificantBits)
