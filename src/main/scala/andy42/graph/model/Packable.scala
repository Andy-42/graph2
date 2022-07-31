package andy42.graph.model

import org.msgpack.core.{MessagePacker, MessageUnpacker}
import zio.{Task, ZIO}

import scala.reflect.ClassTag

trait Packable {
  /**
   * Write self to packer and return packer.
   */
  def pack(packer: MessagePacker): MessagePacker
}

trait Unpackable[T] {
  /**
   * Unpack a T. This can fail, so this is effectful.
   */
  def unpack(unpacker: MessageUnpacker): Task[T]
}

object UnpackOperations {

  def unpackToVector[T: ClassTag](unpackElement: => Task[T], length: Int): Task[Vector[T]] = {

    val a: Array[T] = Array.ofDim[T](length)

    def accumulate(i: Int = 0): Task[Vector[T]] =
      if (i == length)
        ZIO.succeed(a.toVector)
      else
        unpackElement.flatMap { t =>
          a(i) = t
          accumulate(i + 1)
        }

    accumulate()
  }

  def unpackToMap[K, V](unpackEntry: => Task[(K, V)], length: Int): Task[Map[K, V]] = {

    def accumulate(i: Int = 0, r: Map[K, V] = Map.empty): Task[Map[K, V]] =
      if (i == length)
        ZIO.succeed(r)
      else
        unpackEntry.flatMap { entry =>
          accumulate(i + 1, r + entry)
        }

    accumulate()
  }
}