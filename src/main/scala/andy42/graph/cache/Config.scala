package andy42.graph.cache

import zio.config._
import zio.config.magnolia.Descriptor
import zio.config.magnolia.descriptor

final case class Config(
    lruCacheCapacity: Int, // TODO: must be positive
    fractionOfCacheToRetainOnTrim: Float
)

object Config {
  val configDescriptor = descriptor[Config]
}
