package andy42.graph.config

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto.autoUnwrap
import eu.timepit.refined.boolean.And
import eu.timepit.refined.numeric.{GreaterEqual, LessEqual, Positive}
import zio.*
import zio.config.magnolia.deriveConfig

// TODO: The the Refined integration with ZIO 2/Scala 3 is incomplete (4.0.0-RC13) - substitute refined types when fixed

type CacheRetainFraction = Double Refined GreaterEqual[0.0] And LessEqual[1.0]

final case class GraphConfig(
    forkOnEdgeSynchronization: Boolean = true
)

final case class NodeCacheConfig(
    capacity: Int = 1000, // Refined Positive

    // Balance more frequent trims against retaining a useful amount of information in the cache
    fractionToRetainOnNodeCacheTrim: Double /* CacheRetainFraction */ = 0.9,
    currentSnapshotTrimFrequency: Duration = Duration.Zero, // Zero turns off current snapshot purging
    // Don't want to keep snapshots around for long to prevent them becoming tenured in the old heap generation
    fractionOfSnapshotsToRetainOnSnapshotTrim: Double /* CacheRetainFraction */ = 0.1,
    forkOnUpdateAccessTime: Boolean = false, // Might be a useful optimization, but not clear that this is kosher
    forkOnTrim: Boolean = true // Should always have this on at runtime, but false is useful for testing
)

final case class EdgeReconciliationConfig(
    windowSize: Duration = 1.minute,
    windowExpiry: Duration = 5.minutes,
    maximumIntervalBetweenChunks: Duration = 1.minute,
    maxChunkSize: Int = 10000 // Refined Positive
)

// parallelism:
//   > 0 => with that level of parallelism;
//   <= 0 => unbounded parallelism
final case class MatcherConfig(
    fetchSnapshotsParallelism: Int = 0, // default is unbounded
    resolveBindingsParallelism: Int = 4 // TODO: Determine what a suitable default is
)

final case class TracerConfig(
    enabled: Boolean = false,
    host: String = "http://localhost:14250",
    instrumentationScopeName: String = "streaming-graph"
)

final case class AppConfig(
    graph: GraphConfig = GraphConfig(),
    nodeCache: NodeCacheConfig = NodeCacheConfig(),
    edgeReconciliation: EdgeReconciliationConfig = EdgeReconciliationConfig(),
    matcher: MatcherConfig = MatcherConfig(),
    tracer: TracerConfig = TracerConfig()
)

object AppConfig:
  val appConfigDescriptor: Config[AppConfig] = deriveConfig[AppConfig]
  val matcherConfig: Config[MatcherConfig] = deriveConfig[MatcherConfig]
  val graphConfigDescriptor: Config[GraphConfig] = deriveConfig[GraphConfig]
  val lruCacheDescriptor: Config[NodeCacheConfig] = deriveConfig[NodeCacheConfig]
  val edgeReconciliationDescriptor: Config[EdgeReconciliationConfig] = deriveConfig[EdgeReconciliationConfig]
  val tracerConfigDescriptor: Config[TracerConfig] = deriveConfig[TracerConfig]
