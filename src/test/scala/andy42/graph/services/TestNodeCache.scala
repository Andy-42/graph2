package andy42.graph.services

import andy42.graph.model.{Node, NodeId, UnpackFailure}
import zio.*
import zio.stm.TMap

trait TestNodeCache:
  def clear(): UIO[Unit]

  def contents: UIO[Map[NodeId, Node]]

final case class TestNodeCacheLive(cache: TMap[NodeId, Node]) extends NodeCache with TestNodeCache:

  override def get(id: NodeId): IO[UnpackFailure, Option[Node]] = cache.get(id).commit

  override def put(node: Node): IO[UnpackFailure, Unit] = cache.put(node.id, node).commit

  override def startCurrentSnapshotTrimDaemon: UIO[Unit] = ZIO.unit

  override def contents: UIO[Map[NodeId, Node]] = cache.toMap.commit

  override def clear(): UIO[Unit] = cache.removeIfDiscard((_, _) => true).commit

object TestNodeCache:
  val layer: ULayer[NodeCache] =
    ZLayer {
      for cache <- TMap.empty[NodeId, Node].commit
      yield TestNodeCacheLive(cache)
    }
