package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.stm._

import java.time.temporal.ChronoUnit.MILLIS

trait LRUCache {
  def get(id: NodeId): URIO[Clock, Option[Node]]

  def put(node: Node): URIO[Clock, Unit]
}

type AccessTime = Long // epoch millis

case class CacheItem(
    version: Int,
    latest: EventTime,
    packed: PackedNodeContents,
    lastAccess: AccessTime
)

final case class LRUCacheLive(
    config: LRUCacheConfig,
    oldest: TRef[AccessTime], // All items in the cache will have a lastAccess > oldest
    items: TMap[NodeId, CacheItem]
) extends LRUCache {

  override def get(id: NodeId): URIO[Clock, Option[Node]] =
    for {
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      optionNode <- getSTM(id, now).commit
    } yield optionNode

  private def getSTM(
      id: NodeId,
      now: AccessTime
  ): USTM[Option[Node]] =
    for {
      optionItem <- items.updateWith(id)(_.map(_.copy(lastAccess = now)))
    } yield optionItem.map { item =>
      Node(
        id = id,
        version = item.version,
        latest = item.latest,
        packed = item.packed
      )
    }

  override def put(node: Node): URIO[Clock, Unit] =
    for {
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      _ <- trimIfOverCapacity(now).commit
      _ <- putSTM(node, now).commit
    } yield ()

  private def putSTM(
      node: Node,
      now: AccessTime
  ): USTM[Unit] =
    for {
      _ <- items.put(
        node.id,
        CacheItem(
          version = node.version,
          latest = node.latest,
          packed = node.packed,
          lastAccess = now
        )
      )
    } yield ()

  private def trimIfOverCapacity(now: AccessTime): USTM[Unit] =
    ZSTM.ifSTM(items.size.map(_ > config.lruCacheCapacity))(
      onTrue = trim(now),
      onFalse = ZSTM.unit
    )

  private def trim(now: AccessTime): USTM[Unit] =
    for {
      currentOldest <- oldest.get
      newOldest = lastTimeToPurge(currentOldest, now)
      _ <- items.removeIfDiscard { case (_, v) => v.lastAccess < newOldest }
      _ <- oldest.set(newOldest)
    } yield ()

  private def lastTimeToPurge(oldest: AccessTime, now: AccessTime): AccessTime =
    now - Math.ceil((now - oldest) * config.fractionOfCacheToRetainOnTrim).toLong
}

object LRUCache {

  val layer: URLayer[LRUCacheConfig & Clock, LRUCache] =
    ZLayer {
      for {
        config <- ZIO.service[LRUCacheConfig]
        clock <- ZIO.service[Clock]
        now <- clock.currentTime(MILLIS)
        items <- TMap.empty[NodeId, CacheItem].commit
        oldest <- TRef.make(now - 1).commit
      } yield LRUCacheLive(
        config = config,
        oldest = oldest,
        items = items
      )
    }
}
