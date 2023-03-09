package andy42.graph.services

import andy42.graph.model.Node
import andy42.graph.model.NodeId
import andy42.graph.model.StartOfTime
import andy42.graph.model.TestNode
import zio.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed
import zio.test.*

import java.time.temporal.ChronoUnit.MILLIS

object NodeCacheSpec extends ZIOSpecDefault:

  // extract state out of a NodeCacheLive
  extension (cache: NodeCache)
    def size: UIO[Int] = cache match
      case NodeCacheLive(_, _, items) => items.size.commit

    def watermark: UIO[AccessTime] = cache match
      case NodeCacheLive(_, watermark, _) => watermark.get.commit

    def implementation: NodeCacheLive = cache.asInstanceOf[NodeCacheLive]

  override def spec = suite("NodeCache")(
    test(
      "watermark moves forward and items are removed when size exceeds capacity"
    ) {
      for
        cache <- ZIO.service[NodeCache]
        cacheImplementation = cache.implementation

        size0 <- cache.size
        watermark0 <- cache.watermark

        _ <- cache.put(TestNode(0))
        size1 <- cache.size
        watermark1 <- cache.watermark

        _ <- TestClock.adjust(1.millisecond)
        _ <- cache.put(TestNode(1))
        size2 <- cache.size
        watermark2 <- cache.watermark

        // After this put (but before trim):
        // watermark == 0
        // now == 2
        // Trim calculates moveWatermarkForward => 2
        //  intervals = now - watermark + 1 = 2 - 0 + 1 = 3
        //  moveForwardBy = 1 max (3 * 0.5) = 1
        //  nextWatermark => 3 min (0 + 1) = 1
        nowAtTrim <- Clock.currentTime(MILLIS)
        watermarkAtTrim <- cache.watermark
        nextWatermark = cacheImplementation.moveWatermarkForward(now = nowAtTrim, watermark = watermarkAtTrim)
        _ <- TestClock.adjust(1.millisecond)
        _ <- cache.put(TestNode(2))
        size3 <- cache.size
        watermark3 <- cache.watermark

        actualSizes = (size0, size1, size2, size3)
        expectedSizes = (0, 1, 2, 1)

        actualWatermarks = (watermark0, watermark1, watermark2, watermark3)
        expectedWatermarks = (0L, 0L, 0L, 1L)

        logOutput <- ZTestLogger.logOutput
        actualLogMessages = LogOutputExtract.messages(logOutput)
        expectedLogMessages = Chunk("Node cache trimmed")
        actualNowAnnotations = LogOutputExtract.nowAnnotations(logOutput)
        expectedNowAnnotations = Chunk(Some("2"))
        actualCacheItemRetainWatermarkAnnotations = LogOutputExtract.cacheItemRetainWatermark(logOutput)
        expectedCacheItemRetainWatermarkAnnotations = Chunk(Some("1"))
      yield assertTrue(
        nowAtTrim == 1,
        watermarkAtTrim == 0,
        nextWatermark == 1,
        actualSizes == expectedSizes,
        actualWatermarks == expectedWatermarks,
        actualLogMessages == expectedLogMessages,
        actualNowAnnotations == expectedNowAnnotations,
        actualCacheItemRetainWatermarkAnnotations == expectedCacheItemRetainWatermarkAnnotations
      )
    }.provide(
      ZLayer.succeed(
        NodeCacheConfig(capacity = 2, fractionToRetainOnTrim = 0.5, forkOnTrim = false)
      ) >>> NodeCache.layer
    ),
    test("puts from multiple concurrent fibers") {
      for
        cache <- ZIO.service[NodeCache]

        n = 10000

        _ <- TestClock.adjust(1.millisecond) // Avoid warning

        fiber0 <- ZIO
          .foreach(1 until n by 2)(i => cache.put(TestNode(i)))
          .fork
        fiber1 <- ZIO
          .foreach(0 until n by 2)(i => cache.put(TestNode(i)))
          .fork
        fiber2 <- ZIO
          .foreach(0 until n by 3)(i => cache.put(TestNode(i)))
          .fork

        _ <- fiber0.join *> fiber1.join *> fiber2.join

        actualSize <- cache.size
      yield assertTrue(actualSize == n)
    }.provide(
      ZLayer.succeed(
        NodeCacheConfig(capacity = 10000, fractionToRetainOnTrim = 0.75, forkOnTrim = false)
      ) >>> NodeCache.layer
    ),
    test(
      "watermark is moved forward by intervals proportional to configuration"
    ) {
      val n = 100
      val fractionToRetainOnTrim = 0.5

      for
        cache <- ZIO.service[NodeCache]
        cacheImplementation = cache.implementation

        _ <- ZIO.foreach(0 until n) { i =>
          TestClock.adjust(1.millisecond) *> cache.put(TestNode(i))
        }

        watermark <- cache.watermark
        now <- Clock.currentTime(MILLIS)

        intervals = now - watermark
        moveForwardBy = (intervals * fractionToRetainOnTrim).toInt

        nextWatermark = cacheImplementation.moveWatermarkForward(now = now, watermark = watermark)
      yield assertTrue(
        watermark == 0L,
        now == 100L,
        intervals == 100L,
        moveForwardBy == 50,
        nextWatermark == watermark + moveForwardBy
      )
    }.provide(
      ZLayer.succeed(
        NodeCacheConfig(capacity = 100, fractionToRetainOnTrim = 0.5)
      ) >>> NodeCache.layer
    ),
    test(
      "watermark will be moved forward by at least one millisecond if there is room"
    ) {
      val n = 100
      val fractionToRetainOnTrim = 0.0

      for
        cache <- ZIO.service[NodeCache]
        cacheImplementation = cache.implementation

        _ <- ZIO.foreach(0 until n) { i =>
          TestClock.adjust(1.millisecond) *> cache.put(TestNode(i))
        }

        watermark <- cache.watermark
        now <- Clock.currentTime(MILLIS)

        intervals = now - watermark + 1
        moveForwardBy = (intervals * fractionToRetainOnTrim).toLong

        nextWatermark = cacheImplementation.moveWatermarkForward(now = now, watermark = watermark)
      yield assertTrue(
        watermark == 0L,
        now == 100L,
        intervals == 101L,
        moveForwardBy == 0L,
        nextWatermark == watermark + 1 // move forward by at least one since there is room to move
      )
    }.provide(
      ZLayer.succeed(
        NodeCacheConfig(capacity = 100, fractionToRetainOnTrim = 0)
      ) >>> NodeCache.layer
    ),
    test(
      "watermark will never be moved past now"
    ) {
      val n = 100
      val fractionToRetainOnTrim = 0.0 //

      for
        _ <- TestClock.adjust(0.millis)
        now <- Clock.currentTime(MILLIS)

        cache <- ZIO.service[NodeCache]

        // Fill cache just up to capacity
        _ <- ZIO.foreach(0 until n)(i => cache.put(TestNode(i)))
        watermarkAtCapacity <- cache.watermark
        sizeAtCapacity <- cache.size

        // Add one more past capacity
        _ <- cache.put(TestNode(n + 1))
        watermarkPastCapacity <- cache.watermark
        sizePastCapacity <- cache.size

        logOutput <- ZTestLogger.logOutput
        actualLogMessages = LogOutputExtract.messages(logOutput)
        expectedLogMessages = Chunk("Node cache trimmed")
        actualNowAnnotations = LogOutputExtract.nowAnnotations(logOutput)
        expectedNowAnnotations = Chunk(Some("0"))
        actualCacheItemRetainWatermarkAnnotations = LogOutputExtract.cacheItemRetainWatermark(logOutput)
        expectedCacheItemRetainWatermarkAnnotations = Chunk(Some("0"))
      yield assertTrue(
        now == 0L,
        watermarkAtCapacity == 0L,
        sizeAtCapacity == n,
        watermarkPastCapacity == 0L, // There is no room to move the watermark
        sizePastCapacity == 0,
        actualLogMessages == expectedLogMessages,
        actualNowAnnotations == expectedNowAnnotations,
        actualCacheItemRetainWatermarkAnnotations == expectedCacheItemRetainWatermarkAnnotations
      )
    }.provide(
      ZLayer.succeed(
        NodeCacheConfig(capacity = 100, fractionToRetainOnTrim = 0.0)
      ) >>> NodeCache.layer
    )
  ).provideLayer(ZTestLogger.default) @@ timed
