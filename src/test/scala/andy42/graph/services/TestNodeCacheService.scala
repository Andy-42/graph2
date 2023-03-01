package andy42.graph.services

import zio.IO

import andy42.graph.model.Node

import andy42.graph.model.NodeId

import andy42.graph.model.UnpackFailure

import zio._
import zio.stm.TMap

final case class ForgetfulNodeCacheService() extends NodeCache:

  override def get(id: NodeId): IO[UnpackFailure, Option[Node]] = ZIO.succeed(None)

  override def put(node: Node): IO[UnpackFailure, Unit] = ZIO.unit

  override def startSnapshotTrimDaemon: UIO[Unit] = ZIO.unit

object ForgetfulNodeCache:
  val layer: ULayer[NodeCache] = ZLayer.succeed(ForgetfulNodeCacheService())

trait NodeCacheContents:

  def contents: UIO[Map[NodeId, Node]]

final case class RememberingNodeCacheService(cache: TMap[NodeId, Node]) extends NodeCache with NodeCacheContents:

  override def get(id: NodeId): IO[UnpackFailure, Option[Node]] = cache.get(id).commit

  override def put(node: Node): IO[UnpackFailure, Unit] = cache.put(node.id, node).commit

  override def startSnapshotTrimDaemon: UIO[Unit] = ZIO.unit

  override def contents: UIO[Map[NodeId, Node]] = cache.toMap.commit

object RememberingNodeCache:
  val layer: ULayer[NodeCache] =
    ZLayer {
      for cache <- TMap.empty[NodeId, Node].commit
      yield RememberingNodeCacheService(cache)
    }
