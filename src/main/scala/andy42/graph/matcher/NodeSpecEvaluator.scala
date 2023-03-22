package andy42.graph.matcher

import andy42.graph.model.*
import zio.*

import andy42.graph.matcher.NodePredicate
trait NodeSpecEvaluator:
  def matches(id: NodeId, nodeSpec: NodeSpec): NodeIO[Boolean]

case class NodeSpecEvaluatorLive(dataViewCache: MatcherDataViewCache) extends NodeSpecEvaluator:

  override def matches(id: NodeId, nodeSpec: NodeSpec): NodeIO[Boolean] =
    ZIO.forall(nodeSpec.predicates)(evaluatePredicate(id, _))

  private def evaluatePredicate(id: NodeId, predicate: NodePredicate): NodeIO[Boolean] =
    predicate match {
      case NodePredicate.History(_, f) =>
        for history <- dataViewCache.getHistory(id)
        yield f(history)

      case NodePredicate.Snapshot(selector, _, f) =>
        for snapshot <- dataViewCache.getSnapshot(id, selector)
        yield f(snapshot)
    }

object NodeSpecEvaluator:
  def apply(dataViewCache: MatcherDataViewCache): NodeSpecEvaluator =
    NodeSpecEvaluatorLive(dataViewCache)
