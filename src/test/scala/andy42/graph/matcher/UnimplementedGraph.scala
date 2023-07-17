package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.{Graph, InputValidationFailure, NodeMutationInput, SubgraphMatchAtTime}
import zio.IO

case class UnimplementedGraph() extends Graph:

  override def get(id: NodeId): NodeIO[Node] = ???

  override def append(
      time: EventTime,
      changes: Vector[NodeMutationInput]
  ): IO[NodeIOFailure | InputValidationFailure, Seq[SubgraphMatchAtTime]] = ???
