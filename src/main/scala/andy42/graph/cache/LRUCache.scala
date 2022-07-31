package andy42.graph.cache

import zio.*
import zio.stm.*
import andy42.graph.model._
import andy42.graph.model.NodeWithPackedHistory

trait LRUCache {
  def get(id: NodeId): UIO[Option[Node]]

  def put(node: Node): IO[VersionFailure, Unit]
}

case class CacheItem(
    id: NodeId,
    version: Int,
    packed: PackedNode,

    // position in LRU cache TODO: Use direct references and nulls to avoid options and map lookups
    left: Option[NodeId],
    right: Option[NodeId]
)

case class VersionFailure(id: NodeId, existing: Int, updateTo: Int)

final case class LRUCacheLive private (
    capacity: Int,
    currentSize: TRef[Int],
    items: TMap[NodeId, CacheItem],
    startRef: TRef[Option[NodeId]],
    endRef: TRef[Option[NodeId]]
) extends LRUCache {

  override def get(id: NodeId): UIO[Option[Node]] =
    getSTM(id).commitEither

  private def getSTM(id: NodeId): USTM[Option[Node]] =
    for {
      optionItem <- items.get(id)

      _ <- optionItem.fold(ZSTM.unit) { item =>
        removeFromList(item) *> addToStartOfList(id) *> startRef.set(Some(id))
      }

      optionNode = optionItem.map { item =>
        NodeWithPackedHistory(
          id = item.id,
          version = item.version,
          packed = item.packed
        )
      }
    } yield optionNode

  override def put(node: Node): IO[VersionFailure, Unit] =
    putSTM(node).commit

  private def putSTM(node: Node): STM[VersionFailure, Unit] =
    for {
      item <- items.get(node.id)

      _ <- item match {
        case Some(item) =>
          if (node.version == item.version + 1)
            removeFromList(item) // existing entry will be updated in addToStartOfList; no size change
          else
            STM.fail(VersionFailure(id = node.id, existing = item.version, updateTo = node.version))

        case None =>
          if (node.version == 1)
            removeOldestItemIfAtCapacity *> currentSize.update(_ + 1)
          else
            STM.fail(VersionFailure(id = node.id, existing = 0, updateTo = node.version))
      }

      oldOptionStartId <- startRef.get

      _ <- items.put(
        k = node.id,
        v = CacheItem(
          id = node.id,
          version = node.version,
          packed = node.packed,
          left = None,
          right = oldOptionStartId
        )
      )

      _ <- addToStartOfList(oldOptionStartId, node.id)

      _ <- startRef.set(Some(node.id))
    } yield ()

  private def removeFromList(id: NodeId): USTM[Unit] =
    items.get(id).map { optionItem =>
      optionItem.foreach(removeFromList)
    }

  /** Removes an item from the LRU list; does NOT remove from items */
  private def removeFromList(item: CacheItem): USTM[Unit] =
    item match {
      case CacheItem(_, _, _, Some(l), Some(r)) =>
        updateLeftAndRightCacheItems(l, r)

      case CacheItem(_, _, _, Some(l), None) =>
        setNewEnd(l) *> endRef.set(Some(l))

      case CacheItem(_, _, _, None, Some(r)) =>
        setNewStart(r) *> startRef.set(Some(r))

      case CacheItem(_, _, _, None, None) =>
        startRef.set(None) *> endRef.set(None)
    }

  private def addToStartOfList(newStartId: NodeId): USTM[Unit] =
    for {
      oldOptionStartId <- startRef.get
    } yield addToStartOfList(oldOptionStartId, newStartId)

  private def addToStartOfList(
      oldOptionStartId: Option[NodeId],
      newStartId: NodeId
  ): USTM[Unit] =
    oldOptionStartId.fold(ZSTM.unit) { oldStartId =>
      items.get(oldStartId).map { optionOldStartItem =>
        optionOldStartItem.fold(ZSTM.unit) { oldStartItem =>
          items.put(oldStartId, oldStartItem.copy(left = Some(newStartId)))
        }
      }
    }

  private def removeOldestItem: USTM[Unit] =
    for {
      optionOldEndId <- endRef.get

      _ <- optionOldEndId.fold(ZSTM.unit) { oldEndId =>
        removeFromList(oldEndId) *> items.delete(oldEndId)
      }
    } yield currentSize.update(_ - 1)

  private def removeOldestItemIfAtCapacity: USTM[Unit] =
    ZSTM.ifSTM(currentSize.get.map(_ == capacity))(removeOldestItem, STM.unit)

  /** Removes an item from the LRU list by making its left and right cache
    * entries reference each other.
    */
  private def updateLeftAndRightCacheItems(
      leftId: NodeId,
      rightId: NodeId
  ): USTM[Unit] =
    updateLeftCacheItem(leftId, rightId) *> updateRightCacheItem(
      leftId,
      rightId
    )

  private def updateLeftCacheItem(leftId: NodeId, rightId: NodeId): USTM[Unit] =
    items.get(leftId).map { optionLeftCacheItem =>
      optionLeftCacheItem.map { leftCacheItem =>
        items.put(leftId, leftCacheItem.copy(right = Some(rightId)))
      }
    }

  private def updateRightCacheItem(
      leftId: NodeId,
      rightId: NodeId
  ): USTM[Unit] =
    items.get(rightId).map { optionRightCacheItem =>
      optionRightCacheItem.map { rightCacheItem =>
        items.put(rightId, rightCacheItem.copy(left = Some(leftId)))
      }
    }

  private def setNewEnd(newEndId: NodeId): USTM[Unit] =
    items.get(newEndId).map { optionNewEndItem =>
      optionNewEndItem.map { newEndItem =>
        items.put(newEndId, newEndItem.copy(right = None))
      }
    }

  private def setNewStart(newStartId: NodeId): USTM[Unit] =
    items.get(newStartId).map { optionCacheItem =>
      optionCacheItem.map { cacheItem =>
        items.put(newStartId, cacheItem.copy(left = None))
      }
    }
}

object LRUCacheLive {

  // TODO: The capacity should come from config

  def make(capacity: Int): IO[IllegalArgumentException, LRUCacheLive] =
    if (capacity > 0) {
      val makeCacheTransaction = for {
        currentSize <- TRef.make(0)
        items <- TMap.empty[NodeId, CacheItem]
        startRef <- TRef.make(Option.empty[NodeId])
        endRef <- TRef.make(Option.empty[NodeId])
      } yield LRUCacheLive(capacity, currentSize, items, startRef, endRef)

      makeCacheTransaction.commit
    } else ZIO.fail(new IllegalArgumentException("Capacity must be > 0"))
}
