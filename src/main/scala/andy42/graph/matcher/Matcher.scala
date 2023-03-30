package andy42.graph.matcher

import andy42.graph.model.NodeId

case class NodeMatch(
    name: String,
    matchedNode: Option[NodeId]
):
  def isMatched: Boolean = matchedNode.isDefined

type QueryMatch = Vector[NodeMatch]

extension (queryMatch: QueryMatch)
  
  def isMatched = queryMatch.forall(_.isMatched)
