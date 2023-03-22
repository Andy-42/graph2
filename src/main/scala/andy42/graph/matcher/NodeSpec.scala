package andy42.graph.matcher

import scala.collection.mutable.ListBuffer
import andy42.graph.model.*
import scala.util.hashing.MurmurHash3.unorderedHash

import andy42.graph.matcher.{NodeSpec, NodePredicate, SnapshotSelector}
case class NodeLocalSpecBuilder(
    var snapshotSelector: SnapshotSelector = SnapshotSelector.EventTime,
    predicates: ListBuffer[NodePredicate] = ListBuffer.empty
):
  def build: Vector[NodePredicate] = predicates.toVector
  def stage(nodePredicate: NodePredicate): Unit = predicates.append(nodePredicate)

def node(description: String)(body: NodeLocalSpecBuilder ?=> Unit): NodeSpec =
  given builder: NodeLocalSpecBuilder = NodeLocalSpecBuilder()
  body
  NodeSpec(description = description, predicates = builder.build)
