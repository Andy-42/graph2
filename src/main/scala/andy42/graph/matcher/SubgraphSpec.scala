package andy42.graph.matcher

import andy42.graph.matcher
import andy42.graph.model.*
import zio.ZIO

import scala.annotation.tailrec

trait NodePredicate:
  def p: NodeSnapshot ?=> Boolean
  def mermaid: String

/** A specification for the node. This spec constrains using the properties and edges of the node itself, without
  * traversing edges or referencing the the contents of any other node.
  *
  * @param predicates
  *   Predicates that constrain this node.
  */
case class NodeSpec(
    name: String,
    description: String,
    predicates: List[NodePredicate] = Nil
):
  require(name.nonEmpty)

  def withNodePredicate(nodePredicate: NodePredicate): NodeSpec =
    copy(predicates = predicates :+ nodePredicate)

  /* An unconstrained node has no predicates specified. An unconstrained node for every possible NodeId is always
   * considered to exist in the graph, which is significant in optimizing standing query evaluation. */
  def isUnconstrained: Boolean = predicates.isEmpty

  val allPredicatesMatch: NodeSnapshot ?=> Boolean = predicates.forall(_.p)

  def matches(node: Node, time: EventTime): NodeIO[Boolean] =
    if isUnconstrained then ZIO.succeed(true)
    else
      for snapshot <- node.atTime(time)
      yield allPredicatesMatch(using snapshot)

  def matches(id: NodeId, time: EventTime): MatcherDataViewCache ?=> NodeIO[Boolean] =
    if isUnconstrained then ZIO.succeed(true)
    else
      for snapshot <- summon[MatcherDataViewCache].getSnapshot(id, time)
      yield allPredicatesMatch(using snapshot)

  def predicatesMermaid: String = predicates.map("\\n" + _.mermaid).mkString
  def mermaid: String = s"$name[$description$predicatesMermaid]"

def node(name: String, description: String): NodeSpec =
  NodeSpec(name = name, description = description)

trait EdgeKeyPredicate:
  def p(k: String): Boolean
  def mermaid: String

case class EdgeSpec(
    direction: EdgeDirectionPredicate,
    edgeKey: List[EdgeKeyPredicate] = Nil
):
  def withKeyPredicate(p: EdgeKeyPredicate): EdgeSpec =
    copy(edgeKey = edgeKey :+ p)

  def reverse: EdgeSpec =
    copy(direction = direction.reverse)

  def keySpec = edgeKey.map(_.mermaid).mkString("\n")
  def mermaid = s"${direction.from.name} ${direction.mermaid} |$keySpec| ${direction.to.name}"

trait SubgraphPostFilter:
  def p: SnapshotProvider ?=> NodeIO[Boolean]
  def description: String

case class SubgraphSpec(
    name: String,
    edges: List[EdgeSpec],
    filter: Option[SubgraphPostFilter] = None
):

  def where(filter: SubgraphPostFilter): SubgraphSpec =
    copy(filter = Some(filter))

  val nodeMermaid =
    edges
      .flatMap { case edge: EdgeSpec => List(edge.direction.from, edge.direction.to) }
      .map(spec => spec.name -> spec)
      .toMap
      .values
      .map(_.mermaid)
      .mkString("\n")

  val filterMermaid = name + filter.fold("")(filter => ": " + filter.description)
  def subgraphStart = s"subgraph Subgraph[\"$filterMermaid\"]"
  def subgraphEnd = "end"

  def edgeMermaid: String = edges.map(_.mermaid).mkString("\n")
  def mermaid: String = s"flowchart TD\n$subgraphStart\n$nodeMermaid\n$edgeMermaid\n$subgraphEnd"

  // TODO: Validate that names are unique
  val allNodeSpecs: List[NodeSpec] =
    for
      edgeSpec <- edges
      fromAndToSpecs <- List(edgeSpec.direction.from, edgeSpec.direction.to)
    yield fromAndToSpecs

  val allNodeSpecNames: List[String] = allNodeSpecs.map(_.name)

  val nameToNodeSpec: Map[String, NodeSpec] = allNodeSpecs.map(node => node.name -> node).toMap

  val incomingEdges: Map[String, List[EdgeSpec]] =
    allNodeSpecNames.map(nodeSpecName => nodeSpecName -> edges.filter(_.direction.to.name == nodeSpecName)).toMap

  val outgoingEdges: Map[String, List[EdgeSpec]] =
    allNodeSpecNames.map(nodeSpecName => nodeSpecName -> edges.filter(_.direction.from.name == nodeSpecName)).toMap

  def allConnectedNodes(startNode: String): Set[String] =

    def visit(current: String = startNode, visited: Set[String] = Set.empty): Set[String] =
      if visited.contains(current) then visited
      else
        outgoingEdges(startNode)
          .map(_.direction.to.name)
          .foldLeft(visited)((visited, farNode) => visit(farNode, visited))

    visit()

  /** A valid edge spec will have exactly one (fully) connected subgraph */
  def connectedSubgraphs: List[Set[String]] =

    @tailrec
    def accumulate(allSpecs: Set[String] = allNodeSpecNames.toSet, r: List[Set[String]] = Nil): List[Set[String]] =
      if allSpecs.isEmpty then r
      else
        val nextGroup = allConnectedNodes(allSpecs.head)
        val remaining = allSpecs -- nextGroup

        if remaining.isEmpty then nextGroup :: r
        else accumulate(remaining, nextGroup :: r)

    accumulate()

def subgraph(name: String)(edgeSpecs: EdgeSpec*): SubgraphSpec =
  SubgraphSpec(name = name, edges = edgeSpecs.toList)
