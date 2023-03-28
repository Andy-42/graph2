package andy42.graph.matcher

import scala.collection.mutable.ListBuffer
import andy42.graph.model.*
import scala.util.hashing.MurmurHash3.unorderedHash


/**
 * A specification for the node.
 * This spec constrains using the properties and edges of the node itself, without
 * traversing edges or referencing the the contents of any other node.
 *  
  * @param predicates
  *   Predicates that constrain this node.
  */
case class NodeSpec(
    predicates: Vector[NodePredicate]
) extends QueryElement:

  override def spec: String = s"node: ${predicates.map(_.spec).mkString(", ")}"

  /* An unconstrained node has no predicates specified. An unconstrained node for every possible NodeId is always
   * considered to exist in the graph, which is significant in optimizing standing query evaluation. */
  def isUnconstrained: Boolean = predicates.isEmpty


case class NodeLocalSpecBuilder(
    var snapshotSelector: SnapshotSelector = SnapshotSelector.EventTime,
    predicates: ListBuffer[NodePredicate] = ListBuffer.empty
):
  def build: Vector[NodePredicate] = predicates.toVector
  def stage(nodePredicate: NodePredicate): Unit = predicates.append(nodePredicate)

def node(description: String)(body: NodeLocalSpecBuilder ?=> Unit): NodeSpec =
  given builder: NodeLocalSpecBuilder = NodeLocalSpecBuilder()
  body
  NodeSpec(predicates = builder.build)
