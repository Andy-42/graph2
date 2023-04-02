package andy42.graph.matcher

import andy42.graph.matcher
import andy42.graph.model.*
import zio.ZIO

type NodePredicate = NodeSnapshot ?=> Boolean
type EdgeDirectionPredicate = EdgeDirection => Boolean
type EdgeKeyPredicate = String => Boolean

trait SnapshotProvider:
  def get(node: NodeSpec): NodeIO[NodeSnapshot]

/** A specification for the node. This spec constrains using the properties and edges of the node itself, without
  * traversing edges or referencing the the contents of any other node.
  *
  * @param predicates
  *   Predicates that constrain this node.
  */
case class NodeSpec(
    name: String,
    predicates: Vector[NodePredicate]
):
  /* An unconstrained node has no predicates specified. An unconstrained node for every possible NodeId is always
   * considered to exist in the graph, which is significant in optimizing standing query evaluation. */
  def isUnconstrained: Boolean = predicates.isEmpty

  def matches(id: NodeId, time: EventTime): MatcherDataViewCache ?=> NodeIO[Boolean] =
    if (isUnconstrained) ZIO.succeed(true)
    else
      for snapshot <- summon[matcher.MatcherDataViewCache].getSnapshot(id, time)
      yield {
        given NodeSnapshot = snapshot
        predicates.forall((predicate: NodePredicate) => predicate)
      }

def node(name: String)(predicates: NodePredicate*): NodeSpec =
  NodeSpec(name = name, predicates = predicates.toVector)

case class EdgeSpec(
    node1: NodeSpec,
    node2: NodeSpec,
    edgeDirection: EdgeDirectionPredicate,
    edgeKey: Vector[EdgeKeyPredicate] = Vector.empty
):
  def withKeyPredicate(p: EdgeKeyPredicate): EdgeSpec =
    copy(edgeKey = edgeKey :+ p)

case class SubgraphSpec(
    edges: Vector[EdgeSpec],
    filter: Option[SnapshotProvider ?=> NodeIO[Boolean]] = None
):
  def where(f: => SnapshotProvider ?=> NodeIO[Boolean]): SubgraphSpec =
    copy(filter = Some(f))

def subgraph(edgeSpecs: EdgeSpec*): SubgraphSpec =
  SubgraphSpec(edges = edgeSpecs.toVector)
