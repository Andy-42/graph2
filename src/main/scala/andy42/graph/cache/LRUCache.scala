package andy42.graph.cache

import andy42.graph.model._
import zio.*
import zio.stm.*
import andy42.graph.model.PackedNodeContents

trait LRUCache {
  def get(id: NodeId): UIO[Option[Node with PackedNode]]

  def put(node: Node with PackedNode): UIO[Node with PackedNode]
}

case class CacheItem(
    id: NodeId,
    version: Int,
    latest: EventTime,
    packed: PackedNodeContents,
    left: CacheItem, // null if this is the first item in the cache
    right: CacheItem // null if this is the last item in the cache
)

/** A version is created each time an [[EventsAtTime]] are appended to the
  * history. The version number for a [[Node]] corresponds to number of
  * [[EventsAtTime]] in its history. The version number is not associated with a
  * particular [[EventsAtTime]] since they are not necessarily ingested in a
  * monotonically increasing order of event time.
  *
  * The version check is a proof that some other mechanism (e.g.,
  * [[NodeMutation]]) is ensuring that changes to a [[Node]] are serialized.
  */
case class VersionFailure(id: NodeId, existing: Int, updateTo: Int)
    extends IllegalStateException("Version failure")

final case class LRUCacheLive private (
    capacity: Int,
    currentSize: TRef[Int],
    items: TMap[NodeId, CacheItem],
    start: TRef[CacheItem], // null if the cache is empty
    end: TRef[CacheItem] // null if the cache is empty
) extends LRUCache {

  override def get(id: NodeId): UIO[Option[Node with PackedNode]] = {
    for {
      optionItem <- items.get(id)
      startItem <- start.get

      _ <- optionItem.fold(ZSTM.unit) { item =>
        removeFromList(item) *>
          addToStartOfList(oldStart = startItem, newStart = item) *>
          start.set(item)
      }
    } yield optionItem.map { item =>
      Node(
        id = item.id,
        version = item.version,
        latest = item.latest,
        packed = item.packed
      )
    }
  }.commit

  override def put(node: Node with PackedNode): UIO[Node with PackedNode] = {
    for {
      item <- items.get(node.id)

      _ <- item match {
        case Some(item) =>
          if (node.version == item.version + 1)
            // existing entry will be updated in addToStartOfList; no size change
            removeFromList(item)
          else
            ZSTM.die(
              VersionFailure(
                id = node.id,
                existing = item.version,
                updateTo = node.version
              )
            )

        case None =>
          if (node.version == 1)
            removeOldestItemIfAtCapacity() *> currentSize.update(_ + 1)
          else
            STM.die(
              VersionFailure(
                id = node.id,
                existing = 0,
                updateTo = node.version
              )
            )
      }

      oldStart <- start.get

      newItem = CacheItem(
        id = node.id,
        version = node.version,
        latest = node.latest,
        packed = node.packed,
        left = null,
        right = oldStart
      )

      _ <- items.put(k = node.id, v = newItem)
      _ <- addToStartOfList(oldStart = oldStart, newStart = newItem)
      _ <- start.set(newItem)
    } yield node
  }.commit

  /** Removes an item from the LRU list; does NOT remove from items */
  private def removeFromList(item: CacheItem): USTM[Unit] =
    item match {
      case CacheItem(_, _, _, _, null, null) =>
        start.set(null) *> end.set(null)

      case CacheItem(_, _, _, _, l, null) =>
        items.put(l.id, l.copy(right = null)) *> end.set(l)

      case CacheItem(_, _, _, _, null, r) =>
        items.put(r.id, r.copy(left = null)) *> start.set(r)

      case CacheItem(_, _, _, _, l, r) =>
        updateLeftAndRightCacheItems(l, r)
    }

  private def addToStartOfList(
      oldStart: CacheItem, // nullable
      newStart: CacheItem
  ): USTM[Unit] =
    if (oldStart == null)
      ZSTM.unit
    else
      items.put(oldStart.id, oldStart.copy(left = newStart))

  private def removeOldestItem(): USTM[Unit] =
    for {
      oldEnd <- end.get

      _ <-
        if (oldEnd == null || oldEnd.left == null)
          ZSTM.unit
        else
          items.put(oldEnd.left.id, oldEnd.left.copy(right = null))

      _ <-
        if (oldEnd == null)
          ZSTM.unit
        else
          items.delete(oldEnd.id) *> currentSize.update(_ - 1)

    } yield ()

  private def removeOldestItemIfAtCapacity(): USTM[Unit] =
    ZSTM.ifSTM(currentSize.get.map(_ == capacity))(removeOldestItem(), STM.unit)

  /** Removes an item from the LRU list by making its left and right cache
    * entries reference each other.
    */
  private def updateLeftAndRightCacheItems(
      left: CacheItem,
      right: CacheItem
  ): USTM[Unit] =
    for {
      _ <- items.put(left.id, left.copy(right = right))
      _ <- items.put(right.id, right.copy(left = left))
    } yield ()
}

object LRUCacheLive { // Note that this does not follow ZIO convention due to private ctor

  val layer: URLayer[Config, LRUCache] =
    ZLayer {
      for {
        config <- ZIO.service[Config]
        layer <- make(config)
      } yield layer
    }

  def make(config: Config): UIO[LRUCache] = {
    for {
      currentSize <- TRef.make(0)
      items <- TMap.empty[NodeId, CacheItem]
      startRef <- TRef.make(null.asInstanceOf[CacheItem])
      endRef <- TRef.make(null.asInstanceOf[CacheItem])
    } yield LRUCacheLive(
      capacity = config.lruCacheCapacity,
      currentSize = currentSize,
      items = items,
      start = startRef,
      end = endRef
    )
  }.commit
}
