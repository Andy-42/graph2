package andy42.graph.matcher

import io.getquill.Query
import andy42.graph.model.NodeSnapshot
import andy42.graph.model.NodeHistory
import andy42.graph.model.EventTime
import scala.util.hashing.MurmurHash3
import java.time.Instant


enum NodePredicate extends QueryElement:

  case Snapshot(selector: SnapshotSelector, override val spec: String, f: NodeSnapshot => Boolean)
  case History(override val spec: String, f: NodeHistory => Boolean) extends NodePredicate

enum SnapshotSelector:
  case EventTime
  case NodeCurrent
  case AtTime(time: EventTime)

  def makeSpec(predicateSpec: String): String = this match {
    case EventTime    => s"snapshot @ event time => $predicateSpec"
    case NodeCurrent  => s"snapshot @ node current => $predicateSpec"
    case AtTime(time) => s"snapshot @ ${Instant.ofEpochMilli(time)} => $predicateSpec"
  }
