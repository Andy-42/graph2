package andy42.graph.matcher

import andy42.graph.model.*

trait SnapshotProvider:
  def get(node: NodeSpec): NodeIO[NodeSnapshot]

case class SnapshotProviderLive(
    time: EventTime,
    matcherDataViewCache: MatcherDataViewCache,
    subgraphMatch: Map[String, NodeId]
) extends SnapshotProvider:

  override def get(node: NodeSpec): NodeIO[NodeSnapshot] =
    val nodeId = subgraphMatch(node.name)
    matcherDataViewCache.getSnapshot(nodeId, time)
      
