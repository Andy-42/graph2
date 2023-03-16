package andy42.graph.matcher

import scala.collection.mutable.ListBuffer
import andy42.graph.model.*

enum SnapshotSelector:
  case EventTime extends SnapshotSelector
  case NodeCurrent extends SnapshotSelector
  case AtTime(time: EventTime) extends SnapshotSelector

enum NodePredicate:
  case SnapshotPredicate(selector: SnapshotSelector, f: NodeSnapshot => Boolean)
  case HistoryPredicate(f: NodeHistory => Boolean)

case class NodeLocalSpecBuilder(
    var snapshotSelector: SnapshotSelector = SnapshotSelector.EventTime,
    predicates: ListBuffer[NodePredicate] = ListBuffer.empty
):
  def build: Vector[NodePredicate] = predicates.toVector
  def stage(predicate: NodePredicate): Unit = predicates.append(predicate)
  def snapshotFilter(p: NodeSnapshot => Boolean): Unit = stage(NodePredicate.SnapshotPredicate(snapshotSelector, p))
  def historyFilter(p: NodeHistory => Boolean): Unit = stage(NodePredicate.HistoryPredicate(p))

type NodeLocalSpec = NodeLocalSpecBuilder ?=> Unit // TODO: Rename

case class NodeSpec(description: String, predicates: Vector[NodePredicate])

def node(description: String)(body: NodeLocalSpecBuilder ?=> NodeLocalSpec): NodeSpec =
  given builder: NodeLocalSpecBuilder = NodeLocalSpecBuilder()
  body
  NodeSpec(description, builder.build)
