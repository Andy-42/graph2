package andy42.graph.cache

import andy42.graph.model.Edge
import zio._
import zio.config._
import zio.config.magnolia.Descriptor
import zio.config.magnolia.descriptor

// TODO: Edge Reconciliation: windowSize, windowExpiry (both durations)

final case class LRUCacheConfig(
    lruCacheCapacity: Int, // TODO: must be positive
    fractionOfCacheToRetainOnTrim: Float
)

final case class EdgeReconciliationConfig(
    windowSize: Duration,
    windowExpiry: Duration,
    maximumIntervalBetweenChunks: Duration,
    maxChunkSize: Int // TODO: Positive
)

object Config {
  val lruCacheDescriptor = descriptor[LRUCacheConfig]
  val edgeReconciliationDescriptor = descriptor[EdgeReconciliationConfig]
}
