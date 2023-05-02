package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.{Graph, NodeMutationInput}

case class UnimplementedGraph() extends Graph:

  override def get(id: NodeId): NodeIO[Node] = ???

  override def appendFarEdgeEvents(time: EventTime, mutation: NodeMutationInput): NodeIO[Unit] = ???

  override def append(time: EventTime, mutations: Vector[NodeMutationInput]): NodeIO[Unit] = ???
