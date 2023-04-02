package andy42.graph.matcher

import zio.*
import andy42.graph.model.*

/**
 * Extensions to NodeSpec that allow for building general constraints on a node
 * that has already been matched
 */
extension (nodeSpec: NodeSpec)
  
    def getSnapshotForMatchedNode: SnapshotProvider ?=> NodeIO[NodeSnapshot] =
      summon[SnapshotProvider].get(nodeSpec)

    def epochMillis(propertyName: String): SnapshotProvider ?=> NodeIO[Long] =
      for snapshot <- getSnapshotForMatchedNode
      yield snapshot.properties.get(propertyName).get.asInstanceOf[Long] // FIXME: NoSuchElementException, ClassCastException