package andy42.graph.matcher

import andy42.graph.model.{Edge, EdgeDirection, NodeSnapshot}

import scala.util.matching.Regex

trait EdgeDirectionPredicate:
  val from: NodeSpec
  val to: NodeSpec
  def p(edgeDirection: EdgeDirection): Boolean
  def reverse: EdgeDirectionPredicate
  def mermaid: String
// TODO: Some mermaid generation stuff

case class OutgoingDirectedEdgeSpec(from: NodeSpec, to: NodeSpec) extends EdgeDirectionPredicate:
  def p(edgeDirection: EdgeDirection): Boolean = edgeDirection == EdgeDirection.Outgoing
  def reverse: EdgeDirectionPredicate = IncomingDirectedEdgeSpec(from = to, to = from)
  def mermaid = "-->"

case class IncomingDirectedEdgeSpec(from: NodeSpec, to: NodeSpec) extends EdgeDirectionPredicate:
  def p(edgeDirection: EdgeDirection): Boolean = edgeDirection == EdgeDirection.Incoming
  def reverse: EdgeDirectionPredicate = OutgoingDirectedEdgeSpec(from = to, to = from)
  def mermaid = "<--"

case class UndirectedEdgeSpec(from: NodeSpec, to: NodeSpec) extends EdgeDirectionPredicate:
  def p(edgeDirection: EdgeDirection): Boolean = edgeDirection == EdgeDirection.Undirected
  def reverse: EdgeDirectionPredicate = UndirectedEdgeSpec(from = to, to = from)
  def mermaid = "---"

/** Create a new edge specification.
  */
object EdgeSpecs:

  def directedEdge(from: NodeSpec, to: NodeSpec): EdgeSpec =
    EdgeSpec(OutgoingDirectedEdgeSpec(from = from, to = to))

case class EdgeKeyIs(x: String) extends EdgeKeyPredicate:
  override def p(k: String): Boolean = k == x
  override def mermaid: String = x

case class EdgeKeyIsOneOf(s: Set[String]) extends EdgeKeyPredicate:
  override def p(k: String): Boolean = s.contains(k)
  override def mermaid: String = s.toArray.sorted.mkString(" | ")

case class EdgeKeyMatches(r: Regex) extends EdgeKeyPredicate:
  override def p(k: String): Boolean = r.matches(k)
  override def mermaid: String = s"match: $r"

/** Extensions that add an edge predicate to an EdgeSpec. */
extension (edgeSpec: EdgeSpec)
  def edgeKeyIs(s: String) = edgeSpec.withKeyPredicate(EdgeKeyIs(s))
  def edgeKeyIsOneOf(ss: Set[String]) = edgeSpec.withKeyPredicate(EdgeKeyIsOneOf(ss))
  def edgeKeyMatches(r: Regex) = edgeSpec.withKeyPredicate(EdgeKeyMatches(r))

/** Matcher extensions */
extension (edgeSpec: EdgeSpec)
  def someLocalMatchExists: NodeSnapshot ?=> Boolean =
    summon[NodeSnapshot].edges.exists(isLocalMatch)

  def isLocalMatch(edge: Edge): NodeSnapshot ?=> Boolean =
    edgeSpec.direction.p(edge.direction) && edgeSpec.edgeKey.forall(_.p(edge.k))
