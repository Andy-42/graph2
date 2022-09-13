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
  def pack(using packer: MessagePacker): MessagePacker

  def toPacked: Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    this.pack
    packer.toByteArray

trait SeqPacker[T <: Packable]:
  def pack(a: Seq[T])(using packer: MessagePacker): MessagePacker =
    packer.packInt(a.length)
    a.foreach(_.pack)
    packer

  def toPacked(a: Seq[T]): Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker
    packer.packInt(a.length)
    a.foreach(_.pack)
    packer.toByteArray()

trait UncountedSeqPacker[T <: Packable]:
  def pack(a: Seq[T])(using packer: MessagePacker): MessagePacker =
    a.foreach(_.pack)
    packer

  def toPacked(a: Seq[T]): Packed =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    a.foreach(_.pack)
    packer.toByteArray()

trait Unpackable[T: ClassTag]:
  def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, T]

object UnpackOperations:

  // FIXME: Unify these with similar ops in PropertyValue

  def unpackToVector[T: ClassTag](unpackElement: => IO[UnpackFailure, T], length: Int): IO[UnpackFailure, Vector[T]] =

    val a: Array[T] = Array.ofDim[T](length)

    def accumulate(i: Int = 0): IO[UnpackFailure, Vector[T]] =
      if i == length then ZIO.succeed(a.toVector)
      else
        unpackElement.flatMap { t =>
          a(i) = t
          accumulate(i + 1)
        }

    accumulate()

  def unpackToVector[T: ClassTag](
      unpackElement: => IO[UnpackFailure, T],
      hasNext: => Boolean
  ): IO[UnpackFailure, Vector[T]] =
    val buf = ArrayBuffer.empty[T]

    def accumulate(): IO[UnpackFailure, Vector[T]] =
      if hasNext then
        unpackElement.flatMap { t =>
          buf.addOne(t)
          accumulate()
        }
      else ZIO.succeed(buf.toVector)

    accumulate()
