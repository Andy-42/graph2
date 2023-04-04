package andy42.graph.matcher

import andy42.graph.services.NodeMutationOutput
import andy42.graph.matcher.NodeSpec
import andy42.graph.model.*
import zio.*

extension (subgraph: SubgraphSpec)

  def allNodeSpecs: Vector[NodeSpec] =
    subgraph.edges.flatMap { case edge: EdgeSpec => Vector(edge.direction.from, edge.direction.to) }

  // The name and spec for each node to be matched
  def resolutionAgenda: Map[String, NodeSpec] =
    subgraph.allNodeSpecs.map(node => node.name -> node).toMap // TODO: This skips a bunch of validation: e.g., connectedness
    
  def edgesBySource: Map[String, List[NodeSpec]] = ???  

case class Resolution(time: EventTime, node: Node, spec: NodeSpec /*, agenda: Map[String, NodeId] */)

object Matcher:

  def initialResolution(time: EventTime, nodes: Vector[Node], subgraph: SubgraphSpec): NodeIO[Vector[Resolution]] =

    val agenda = subgraph.resolutionAgenda
    
    val nodeCrossSpec =
      for
        node <- nodes
        spec <- agenda.values
      yield (node, spec)

    for initialMatches <- ZIO.filter(nodeCrossSpec) { case (node, spec) => spec.matches(node, time) }
    yield initialMatches.map { case (node, spec) => Resolution(time, node, spec)}

