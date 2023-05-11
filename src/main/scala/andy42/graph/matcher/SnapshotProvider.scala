package andy42.graph.matcher

import andy42.graph.model.*

trait SnapshotProvider:
  def get(node: NodeSpec): NodeIO[NodeSnapshot]

// TODO: This can be merged with MatcherNodeCache?
case class SnapshotProviderLive(
    time: EventTime,
    matcherSnapshotCache: MatcherSnapshotCache,
    subgraphMatch: Map[String, NodeId]
) extends SnapshotProvider:

  override def get(node: NodeSpec): NodeIO[NodeSnapshot] =
    val nodeId = subgraphMatch(node.name)
    matcherSnapshotCache.get(nodeId)
