package andy42.graph.model

import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.ValueType
import zio._

import java.io.IOException
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait Packable:
  /** Write self to packer and return packer.
    */
  def pack(packer: MessagePacker): MessagePacker

sealed trait UnpackFailure extends Throwable

// A failure generated by MessagePack
final case class DecodingFailure(ioe: IOException) extends UnpackFailure
// Got an event type that doesn't correspond to a known Event type
final case class UnexpectedEventDiscriminator(discriminator: Int) extends UnpackFailure
// Got a org.msgpack.value.ValueType for a structure type (Map, Set) where a scalar type was expected
final case class UnexpectedScalarValueType(valueType: ValueType) extends UnpackFailure

object UnpackFailure:
  val refine: PartialFunction[UnpackFailure | Throwable, UnpackFailure] =
    case unpackFailure: UnpackFailure => unpackFailure
    case ioe: IOException             => DecodingFailure(ioe)

trait Unpackable[T]:

  /** Unpack a T. This can fail, so this is effectful.
    */
  def unpack(unpacker: MessageUnpacker): IO[UnpackFailure, T]

object UnpackOperations:

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

  def unpackToMap[K, V](unpackEntry: => IO[UnpackFailure, (K, V)], length: Int): IO[UnpackFailure, Map[K, V]] =

    def accumulate(i: Int = 0, r: Map[K, V] = Map.empty): IO[UnpackFailure, Map[K, V]] =
      if i == length then ZIO.succeed(r)
      else
        unpackEntry.flatMap { entry =>
          accumulate(i + 1, r + entry)
        }

    accumulate()
