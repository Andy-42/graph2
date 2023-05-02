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
    name: NodeSpecName,
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

def node(name: String, description: String = ""): NodeSpec =
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

  def keySpec: String = edgeKey.map(_.mermaid).mkString("\n")
  def mermaid = s"${direction.from.name} ${direction.mermaid} |$keySpec| ${direction.to.name}"

trait SubgraphPostFilter:
  def p: SnapshotProvider ?=> NodeIO[Boolean]
  def description: String

/** A SubgraphSpec specifies a pattern that can be matched against the graph.
  *
  * Since a NodeSpec can only be included in the subgraph via an edge, this implies that this kind of match
  * specification is incapable of expressing a single node by itself (at least not without a reflexive edge to itself).
  * If you considered single-node matches interesting, you would need to implement a different sort of matching spec.
  *
  * @param name
  *   Identifies this SubgraphSpec in the output match stream.
  * @param edges
  *   The edges between nodes in the subgraph.
  * @param filter
  *   A filter that can be applied to a subgraph that can be applied to a SubgraphMatch that has otherwise matched on
  *   node and edge predicates.
  */
case class SubgraphSpec(
    name: String,
    edges: List[EdgeSpec],
    filter: Option[SubgraphPostFilter] = None
):

  def where(filter: SubgraphPostFilter): SubgraphSpec =
    copy(filter = Some(filter))

  val nodeMermaid: String =
    edges
      .flatMap { case edge: EdgeSpec => List(edge.direction.from, edge.direction.to) }
      .map(spec => spec.name -> spec)
      .toMap
      .values
      .map(_.mermaid)
      .mkString("\n")

  val filterMermaid: _root_.java.lang.String = name + filter.fold("")(filter => ": " + filter.description)
  def subgraphStart = s"subgraph Subgraph[\"$filterMermaid\"]"
  def subgraphEnd = "end"

  def edgeMermaid: String = edges.map(_.mermaid).mkString("\n")
  def mermaid: String = s"flowchart TD\n$subgraphStart\n$nodeMermaid\n$edgeMermaid\n$subgraphEnd"

  val allHalfEdges: List[EdgeSpec] = edges ++ edges.map(_.reverse)

  private def allNodeSpecsIncludingDuplicates: List[NodeSpec] =
    for
      edgeSpec <- edges
      fromAndToSpecs <- List(edgeSpec.direction.from, edgeSpec.direction.to)
    yield fromAndToSpecs

  // TODO: Validate that names of distinct NodeSpecs are are unique
  val allNodeSpecs: List[NodeSpec] =
    allNodeSpecsIncludingDuplicates
      .map(nodeSpec => nodeSpec.name -> nodeSpec)
      .toMap
      .toList
      .sortBy(_._1)
      .map((x: (NodeSpecName, NodeSpec)) => x._2)

  val allNodeSpecNames: List[NodeSpecName] = allNodeSpecs.map(_.name).sorted

  val nameToNodeSpec: Map[NodeSpecName, NodeSpec] = allNodeSpecs.map(node => node.name -> node).toMap

  val incomingEdges: Map[NodeSpecName, List[EdgeSpec]] =
    allNodeSpecNames
      .map(nodeSpecName =>
        nodeSpecName -> allHalfEdges
          .filter(_.direction.to.name == nodeSpecName)
          .sortBy(edgeSpec => (edgeSpec.direction.from.name, edgeSpec.direction.to.name))
      )
      .toMap

  val outgoingEdges: Map[NodeSpecName, List[EdgeSpec]] =
    allNodeSpecNames
      .map(nodeSpecName => nodeSpecName -> allHalfEdges.filter(_.direction.from.name == nodeSpecName))
      .toMap

  /** Validate that all NodeSpecs have a unique name.
    * @return
    *   Any NodeSpec names that are duplicated. This list will be unique and sorted.
    */
  def duplicateNodeSpecNames: List[NodeSpecName] =

    @tailrec def accumulate(
        nodeSpecs: List[NodeSpec] = allNodeSpecsIncludingDuplicates,
        x: Map[NodeSpecName, NodeSpec] = Map.empty[NodeSpecName, NodeSpec],
        duplicates: List[NodeSpecName] = Nil
    ): List[NodeSpecName] =
      nodeSpecs match {
        case nodeSpec :: xs =>
          if !x.contains(nodeSpec.name) then accumulate(xs, x + (nodeSpec.name -> nodeSpec), duplicates)
          else if x(nodeSpec.name) eq nodeSpec then accumulate(xs, x, duplicates)
          else accumulate(xs, x, nodeSpec.name :: duplicates)

        case _ => duplicates.distinct.sorted
      }

    accumulate()

  final def allConnectedNodes(
      current: NodeSpecName,
      visited: Set[NodeSpecName] = Set.empty
  ): Set[NodeSpecName] =
    if visited.contains(current) then visited
    else
      outgoingEdges(current)
        .map(_.direction.to.name)
        .foldLeft(visited + current)((visited, farNode) => allConnectedNodes(farNode, visited))

  /** A valid edge spec will have exactly one (fully) connected subgraph */
  @tailrec
  final def connectedSubgraphs(
      remainingNodeSpecs: Set[NodeSpecName] = allNodeSpecNames.toSet,
      r: Set[Set[NodeSpecName]] = Set.empty
  ): Set[Set[NodeSpecName]] =
    if remainingNodeSpecs.isEmpty then r
    else
      val nextGroup = allConnectedNodes(remainingNodeSpecs.head)
      connectedSubgraphs(remainingNodeSpecs -- nextGroup, r + nextGroup)

def subgraph(name: String)(edgeSpecs: EdgeSpec*): SubgraphSpec =
  SubgraphSpec(name = name, edges = edgeSpecs.toList)
