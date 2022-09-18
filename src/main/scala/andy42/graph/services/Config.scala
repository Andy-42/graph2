package andy42.graph.services

import zio.*
import zio.config.*
import zio.config.magnolia.Descriptor
import zio.config.magnolia.descriptor

final case class NodeCacheConfig(
    capacity: Int, // TODO: must be positive
    fractionToRetainOnTrim: Float,

    snapshotPurgeFrequency: Duration,
    fractionOfSnapshotsToRetainOnSnapshotPurge: Float
)

final case class EdgeReconciliationConfig(
    windowSize: Duration,
    windowExpiry: Duration,
    maximumIntervalBetweenChunks: Duration,
    maxChunkSize: Int // TODO: Positive
)

object Config:
  val lruCacheDescriptor = descriptor[NodeCacheConfig]
  val edgeReconciliationDescriptor = descriptor[EdgeReconciliationConfig]
