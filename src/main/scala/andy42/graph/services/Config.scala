package andy42.graph.services

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto.autoUnwrap
import eu.timepit.refined.boolean.And
import eu.timepit.refined.numeric.{GreaterEqual, LessEqual, Positive}
import zio.{ConfigProvider, *}
import zio.config.magnolia.deriveConfig

// TODO: The the Refined integration with ZIO 2/Scala 3 is incomplete (4.0.0-RC13) - substitute refined types when fixed

type CacheRetainFraction = Double Refined GreaterEqual[0.0] And LessEqual[1.0]

final case class NodeCacheConfig(
    capacity: Int, // Refined Positive

    // Balance more frequent trims against retaining a useful amount of information in the cache
    fractionToRetainOnNodeCacheTrim: Double /* CacheRetainFraction */ = 0.9,

    currentSnapshotTrimFrequency: Duration = Duration.Zero, // Zero turns off current snapshot purging
    // Don't want to keep snapshots around for long to prevent them becoming tenured in the old heap generation
    fractionOfSnapshotsToRetainOnSnapshotTrim: Double /* CacheRetainFraction */ = 0.1,

    forkOnUpdateAccessTime: Boolean = false, // Might be a useful optimization, but not clear that this is kosher
    forkOnTrim: Boolean = true // Should always have this on at runtime, but false is useful for testing
)

final case class EdgeReconciliationConfig(
    windowSize: Duration,
    windowExpiry: Duration,
    maximumIntervalBetweenChunks: Duration,
    maxChunkSize: Int // Refined Positive
)

object Config:
  val lruCacheDescriptor: Config[NodeCacheConfig] = deriveConfig[NodeCacheConfig]
  val edgeReconciliationDescriptor: Config[EdgeReconciliationConfig] = deriveConfig[EdgeReconciliationConfig]
