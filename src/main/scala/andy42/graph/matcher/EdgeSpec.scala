package andy42.graph.matcher

import scala.collection.mutable.ListBuffer
import andy42.graph.model.*
import scala.util.hashing.MurmurHash3.unorderedHash

case class EdgeDirectionPredicate(
    override val spec: String, 
    f: EdgeDirection => Boolean) extends QueryElement

case class EdgeKeyPredicate(
    override val spec: String,
    f: String => Boolean // Match the edge key
) extends QueryElement

/** Predicates that constrain the relationships between two nodes.
  *
  * TODO: This doesn't support comparing the relationships beween two nodes, as in the time comparisons between two
  * nodes, as in the APT Detection example:
  * {{{
  *        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
  *          (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
  *        WHERE id(f) = $that.data.fileId
  *          AND e1.type = "WRITE"
  *          AND e2.type = "READ"
  *          AND e3.type = "DELETE"
  *          AND e4.type = "SEND"
  *          AND e1.time < e2.time
  *          AND e2.time < e3.time
  *          AND e2.time < e4.time
  * }}}
  */
case class EdgeSpec(
    ordinal: Int = 0,
    node1: NodePredicate,
    node2: NodePredicate,
    directionPredicate: EdgeDirectionPredicate,
    edgeKeyPredicate: Option[EdgeKeyPredicate]
) extends QueryElement:

  override def spec: String = s"edge: ${node1.fingerprint} ${directionPredicate.spec} ${node2.fingerprint}" +
    s"${edgeKeyPredicate.fold("")(p => s" key: ${p.spec}")}"

case class EdgeSpecs(
    edgeSpecs: Vector[EdgeSpec]
) extends QueryElement:
  override def spec: String = s"edges: ${edgeSpecs.map(_.spec).mkString(", ")}"

case class EdgeSpecsBuilder(
    predicates: ListBuffer[EdgeSpec] = ListBuffer.empty
):
  def build: Vector[EdgeSpec] = predicates.toVector
  def stage(edgeSpec: EdgeSpec): Unit = predicates.append(edgeSpec)

def edges(description: String)(body: EdgeSpecsBuilder ?=> Unit): EdgeSpecs =
  given builder: EdgeSpecsBuilder = EdgeSpecsBuilder()
  body
  EdgeSpecs(edgeSpecs = builder.build)
