package andy42.graph.matcher

import io.getquill.Query
import andy42.graph.model.NodeSnapshot
import andy42.graph.model.NodeHistory
import andy42.graph.model.EventTime
import scala.util.hashing.MurmurHash3
import java.time.Instant

type NodeSpecId = Int

/** 
 * 
 * @param spec
  *   Specification for the matcher - human readable and basis for the fingerprint.
  * @param fingerprint
  *   A more compact integer to (mostly) uniquely identify the node predicate logic.
  */
trait QueryElement:
  def spec: String
  def fingerprint: Int = MurmurHash3.stringHash(spec)

/** @param description
  *   A description of the node.
  * @param id
  *   A unique identifier for this node within a query - will be renumbered during compile.
  * @param predicates
  *   Predicates that constrain this node.
  */
case class NodeSpec(
    description: String,
    id: NodeSpecId = 0,
    predicates: Vector[NodePredicate]
) extends QueryElement:

  override def spec: String = s"nodes: ${predicates.map(_.spec).mkString(", ")}"

  /* An unconstrained node has no predicates specified. An unconstrained node for every possible NodeId is always
   * considered to exist in the graph, which is significant in optimizing standing query evaluation. */
  def isUnconstrained: Boolean = predicates.isEmpty

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
