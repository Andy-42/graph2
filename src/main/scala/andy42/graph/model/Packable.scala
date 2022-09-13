package andy42.graph.model

import org.msgpack.core._
import org.msgpack.value.ValueType
import zio._

import java.io.IOException
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

type Packed = Array[Byte]

trait Packable:
  /** Write self to packer and return packer.
    */
  def pack(using packer: MessagePacker): Unit

  def toPacked: Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    this.pack
    packer.toByteArray()

trait CountedSeqPacker[T <: Packable]:
  def pack(a: Seq[T])(using packer: MessagePacker): Unit =
    packer.packInt(a.length)
    a.foreach(_.pack)

  def toPacked(a: Seq[T]): Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    packer.packInt(a.length)
    a.foreach(_.pack)
    packer.toByteArray()

trait UncountedSeqPacker[T <: Packable]:
  def pack(a: Seq[T])(using packer: MessagePacker): Unit =
    a.foreach(_.pack)

  def toPacked(a: Seq[T]): Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    a.foreach(_.pack)
    packer.toByteArray()

trait Unpackable[T: ClassTag]:
  def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, T]

object UnpackOperations:

  def unpackCountedToSeq[T: ClassTag](
      unpackElement: => IO[UnpackFailure | Throwable, T],
      length: Int
  ): IO[UnpackFailure, Seq[T]] = {

    val a: Array[T] = Array.ofDim[T](length)

    def accumulate(i: Int = 0): IO[UnpackFailure | Throwable, Seq[T]] =
      if i == length then ZIO.succeed(a)
      else
        unpackElement.flatMap { t =>
          a(i) = t
          accumulate(i + 1)
        }

    accumulate()
  }.refineOrDie(UnpackFailure.refine)

  def unpackUncountedToSeq[T: ClassTag](
      unpackElement: => IO[UnpackFailure | Throwable, T],
      hasNext: => Boolean
  ): IO[UnpackFailure, Seq[T]] = {
    val buf = ArrayBuffer.empty[T]

    def accumulate(): IO[UnpackFailure | Throwable, Seq[T]] =
      if hasNext then
        unpackElement.flatMap { t =>
          buf.addOne(t)
          accumulate()
        }
      else ZIO.succeed(buf.toSeq)

    accumulate()
  }.refineOrDie(UnpackFailure.refine)
