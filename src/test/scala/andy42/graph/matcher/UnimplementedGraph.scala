package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.{Graph, NodeMutationInput}
import zio.UIO

case class UnimplementedGraph() extends Graph:

  override def get(id: NodeId): NodeIO[Node] = ???

  override def registerStandingQuery(subgraphSpec: SubgraphSpec): UIO[Unit] = ???

  override def append(time: EventTime, changes: Vector[NodeMutationInput]): NodeIO[Unit] = ???
