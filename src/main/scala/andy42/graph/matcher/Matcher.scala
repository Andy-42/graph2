package andy42.graph.matcher

import andy42.graph.services.NodeMutationOutput
import andy42.graph.matcher.NodeSpec
import andy42.graph.model.*
import zio.*


case class Resolution(time: EventTime, node: Node, spec: NodeSpec /*, agenda: Map[String, NodeId] */)

object Matcher:

  def initialResolution(time: EventTime, nodes: Vector[Node], subgraph: SubgraphSpec): NodeIO[Vector[Resolution]] =
    
    val allNodeSpecs = subgraph.edges.allNodeSpecs
    
    val nodeCrossSpec =
      for
        node <- nodes
        spec <- allNodeSpecs
      yield (node, spec)

    for initialMatches <- ZIO.filter(nodeCrossSpec) { case (node, spec) => spec.matches(node, time) }
    yield initialMatches.map { case (node, spec) => Resolution(time, node, spec)}

