package andy42.graph.services

import zio.*
import zio.config.*
import zio.config.magnolia.Descriptor
import zio.config.magnolia.descriptor

final case class NodeCacheConfig(
    capacity: Int, // TODO: must be positive
    
    fractionToRetainOnNodeCacheTrim: Double =
      0.9, // Balance more frequent trims against retaining a useful amount of information in the cache

    currentSnapshotTrimFrequency: Duration = Duration.Zero, // Zero turns off current snapshot purging
    fractionOfSnapshotsToRetainOnSnapshotTrim: Double = 
      0.1, // Don't want to keep snapshots around for long to prevent them becoming tenured in the old heap generation

    forkOnUpdateAccessTime: Boolean = false, // Might be a useful optimization, but not clear that this is kosher
    forkOnTrim: Boolean = true // TODO: positive; should always have this on, but false is useful for testing
)

final case class EdgeReconciliationConfig(
    windowSize: Duration,
    windowExpiry: Duration,
    maximumIntervalBetweenChunks: Duration,
    maxChunkSize: Int // TODO: Positive
)

object Config:
  val lruCacheDescriptor: ConfigDescriptor[NodeCacheConfig] = descriptor[NodeCacheConfig]
  val edgeReconciliationDescriptor: ConfigDescriptor[EdgeReconciliationConfig] = descriptor[EdgeReconciliationConfig]
