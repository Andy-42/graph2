package andy42.graph.cache

import zio.stm.{STM, TMap, TRef, USTM, ZSTM}
import zio.*

trait LRUCache {
  def get(id: NodeId): IO[NoSuchElement, Option[Node]]

  def put(node: Node): UIO[Unit]
}

case class CacheItem(id: NodeId,
                     version: Int,

                     // Payload
                     history: Array[Byte],
                     current: Array[Byte],

                     // position in LRU cache
                     left: Option[NodeId],
                     right: Option[NodeId])

// TODO: This is probably a trait defined somewhere else
case class Node(id: NodeId, version: Int, current: Array[Byte], history: Array[Byte])

sealed trait LRUCacheFailure

trait InconsistentStateFailure extends LRUCacheFailure // An inconsistent state that should cause transaction retry

trait IllegalArgumentFailure extends LRUCacheFailure // An internal state that should not occur

case class NoSuchElement(id: NodeId) extends InconsistentStateFailure

case class InconsistentState() extends InconsistentStateFailure

case class VersionFailure(existing: Int, updateTo: Int) extends IllegalArgumentFailure


final case class ConcurrentLRUCache private(capacity: Int,
                                            currentSize: TRef[Int],
                                            items: TMap[NodeId, CacheItem],
                                            startRef: TRef[Option[NodeId]],
                                            endRef: TRef[Option[NodeId]]
                                           ) {

  def get(id: NodeId): IO[NoSuchElement | InconsistentStateFailure, Option[Node]] =
    getSTM(id).commitEither // TODO: is commitEither correct?

  private def getSTM(id: NodeId): STM[NoSuchElement | InconsistentStateFailure, Option[Node]] =
    for {
      optionItem <- items.get(id)

      _ <- optionItem match {
        case Some(item) => removeFromList(item) *> addToStartOfList(id)
        case None => STM.unit
      }
    } yield optionItem.map { item =>
      Node(id = item.id,
        version = item.version,
        current = item.current,
        history = item.history)
    }

  def put(node: Node): IO[VersionFailure | NoSuchElement | InconsistentStateFailure, Unit] =
    putSTM(node).commit // TODO: ???

  private def putSTM(node: Node): STM[VersionFailure | NoSuchElement | InconsistentStateFailure, Unit] =
    for {
      item <- items.get(node.id)

      _ <- item match {
        case Some(item) =>
          if (node.version == item.version + 1)
            removeFromList(item) // existing entry will be updated in addToStartOfList; no size change
          else
            STM.fail(VersionFailure(existing = item.version, updateTo = node.version))

        case None =>
          if (node.version == 1)
            removeOldestItemIfAtCapacity *> currentSize.update(_ + 1)
          else
            STM.fail(VersionFailure(existing = 0, updateTo = node.version))
      }

      _ <- addToItems(node)
      _ <- addToStartOfList(node.id)
    } yield ()

  private def removeFromList(id: NodeId): STM[NoSuchElement, Unit] =
    for {
      item <- getExistingCacheItem(id)
      _ <- removeFromList(item)
    } yield ()

  /** Removes an item from the LRU list; does NOT remove from items */
  private def removeFromList(item: CacheItem): STM[NoSuchElement, Unit] =
    (item.left, item.right) match {
      case (Some(l), Some(r)) => updateLeftAndRightCacheItems(l, r)
      case (Some(l), None) => setNewEnd(l)
      case (None, Some(r)) => setNewStart(r)
      case (None, None) => clearStartAndEnd
    }

  private def addToStartOfList(id: NodeId): STM[InconsistentStateFailure, Unit] =
    for {
      oldOptionStartId <- startRef.get

      _ <- oldOptionStartId match {
        case Some(oldStart) =>
          getExistingCacheItem(oldStart).flatMap { oldStartCacheItem =>
            items.put(oldStart, oldStartCacheItem.copy(left = Some(id)))
          }

        case None => STM.fail(InconsistentState())
      }

      _ <- startRef.set(Some(id))
    } yield ()

  private def addToItems(node: Node): STM[NoSuchElement, Unit] =
    for {
      oldOptionStartId <- startRef.get

      _ <- items.put(
        k = node.id,
        v = CacheItem(node.id, node.version, node.history, node.current, left = None, right = oldOptionStartId)
      )
    } yield ()

  /**
   * Get an existing cache item.
   * FIXME: This is used in contexts where non-existent item is a logic failure - that should not be retried!
   */
  private def getExistingCacheItem(id: NodeId): STM[NoSuchElement, CacheItem] =
    items.get(id).someOrFail(NoSuchElement(id))

  private def removeOldestItem: STM[NoSuchElement, Unit] =
    for {
      endId <- endRef.get

      _ <- endId match {
        case Some(oldEndId) => removeFromList(oldEndId) *> items.delete(oldEndId)
        case None => STM.unit
      }

      _ <- currentSize.update(_ - 1)
    } yield ()

  private def removeOldestItemIfAtCapacity: STM[NoSuchElement, Unit] =
    for {
      _ <- ZSTM.ifSTM(currentSize.get.map(_ == capacity))(removeOldestItem, STM.unit)
    } yield ()

  /**
   * Removes an item from the LRU list by making its left and right cache entries reference each other.
   */
  private def updateLeftAndRightCacheItems(left: NodeId, right: NodeId): STM[NoSuchElement, Unit] =
    for {
      leftCacheItem <- getExistingCacheItem(left)
      rightCacheItem <- getExistingCacheItem(right)
      _ <- items.put(left, leftCacheItem.copy(right = Some(right)))
      _ <- items.put(right, rightCacheItem.copy(left = Some(left)))
    } yield ()

  private def setNewEnd(newEnd: NodeId): STM[NoSuchElement, Unit] =
    for {
      cacheItem <- getExistingCacheItem(newEnd)
      _ <- items.put(newEnd, cacheItem.copy(right = None)) *> endRef.set(Some(newEnd))
    } yield ()

  private def setNewStart(newStart: NodeId): STM[NoSuchElement, Unit] =
    for {
      cacheItem <- getExistingCacheItem(newStart)
      _ <- items.put(newStart, cacheItem.copy(left = None)) *> startRef.set(Some(newStart))
    } yield ()

  /** Make the LRU list empty by empyting the LRU list endpoints. */
  private def clearStartAndEnd: USTM[Unit] =
    startRef.set(None) *> endRef.set(None)
}

object ConcurrentLRUCache {

  def make(capacity: Int): IO[IllegalArgumentException, ConcurrentLRUCache] =
    if (capacity > 0)
      (for {
        currentSize <- TRef.make(0)
        items <- TMap.empty[NodeId, CacheItem]
        startRef <- TRef.make(Option.empty[NodeId])
        endRef <- TRef.make(Option.empty[NodeId])
      } yield new ConcurrentLRUCache(capacity, currentSize, items, startRef, endRef)).commit
    else
      ZIO.fail(new IllegalArgumentException("Capacity must be > 0"))
}