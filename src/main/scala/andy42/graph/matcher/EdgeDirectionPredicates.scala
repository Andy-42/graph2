package andy42.graph.matcher

import andy42.graph.model.{Edge, EdgeDirection}

object EdgeDirectionPredicates:

  def directedEdge(from: NodeSpec, to: NodeSpec): EdgeDirectionPredicate =
    EdgeDirectionPredicate(
      node1 = from,
      node2 = to,
      directionSpec = s"directedEdge(from=${from.fingerprint}, to=${to.fingerprint}",
      f = (edgeDirection: EdgeDirection) => edgeDirection == EdgeDirection.Outgoing
    )
