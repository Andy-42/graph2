package andy42.graph.matcher

import andy42.graph.model.*

case class NodeHasPropertyPredicate(k: String) extends NodePredicate:
  override def p: NodeSnapshot ?=> Boolean = summon[NodeSnapshot].properties.contains(k)
  override def mermaid: String = s"property: $k"

case class NodeHasPropertyWithValue(k: String, v: PropertyValueType) extends NodePredicate:
  override def p: NodeSnapshot ?=> Boolean = summon[NodeSnapshot].properties.get(k) == Some(v)
  override def mermaid: String = s"property: $k = $v"

case class IsLabeled(label: String) extends NodePredicate:
  override def p: NodeSnapshot ?=> Boolean = summon[NodeSnapshot].properties.get(label) == Some(())
  override def mermaid: String = s"label: $label"

extension (nodeSpec: NodeSpec)
  def hasProperty(k: String): NodeSpec =
    nodeSpec.withNodePredicate(NodeHasPropertyPredicate(k))

  def hasProperty(k: String, v: PropertyValueType): NodeSpec =
    nodeSpec.withNodePredicate(NodeHasPropertyWithValue(k, v))

  def isLabeled(label: String): NodeSpec =
    nodeSpec.withNodePredicate(IsLabeled(label))

extension (nodeSpec: NodeSpec)
  def allPredicatesMatch: NodeSnapshot ?=> Boolean =
    nodeSpec.predicates.forall(_.p)
