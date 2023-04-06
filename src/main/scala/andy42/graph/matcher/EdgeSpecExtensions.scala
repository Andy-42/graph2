package andy42.graph.matcher

import scala.annotation.tailrec

extension(edges: Vector[EdgeSpec])

  def allNodeSpecs: List[NodeSpec] =
    for
      edgeSpec <- edges.toList
      fromAndToSpecs <- List(edgeSpec.direction.from, edgeSpec.direction.to)
    yield fromAndToSpecs

  def allNodeSpecNames: List[String] = allNodeSpecs.map(_.name)
  
  // TODO: Validate that names are unique

  def nameToNodeSpec: Map[String, NodeSpec] =
    allNodeSpecs.map(node => node.name -> node).toMap

  def incomingEdges: Map[String, List[String]] =
    allNodeSpecNames.map(name =>
      name -> edges.filter(_.direction.to.name == name).map(_.direction.from.name).toList
    ).toMap

  def outgoingEdges: Map[String, List[String]] =
    allNodeSpecNames.map(name =>
      name -> edges.filter(_.direction.from.name == name).map(_.direction.to.name).toList
    ).toMap

  def allConnectedNodes(startNode: String): Set[String] =
    val outgoing = outgoingEdges
    
    def accumulate(current: String = startNode, visited: Set[String] = Set.empty): Set[String] =
      if visited.contains(current) then visited
      else outgoing(current).foldLeft(visited + current)((z, otherNode) => accumulate(otherNode, z))
      
    accumulate()

  /**
   * All the connected groups of connected subgraphs.
   * A valid edge spec will have exactly one (fully) connected subgraph
   */
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
        



