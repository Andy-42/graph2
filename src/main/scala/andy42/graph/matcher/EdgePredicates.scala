package andy42.graph.matcher

import andy42.graph.model.EdgeDirection

import scala.util.hashing.MurmurHash3
import andy42.graph.model.NodeId

import scala.annotation.targetName
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

case class EdgeSpecs(
                      edgeSpecs: Vector[EdgeDirectionPredicate]
                    ) extends QueryElement:
  override def spec: String = s"edges: ${edgeSpecs.map(_.spec).mkString(", ")}"

case class EdgeDirectionPredicate(
    node1: NodeSpec,
    node2: NodeSpec,
    directionSpec: String,
    f: EdgeDirection => Boolean,
    edgeKeyPredicates: Vector[EdgeKeyPredicate] = Vector.empty
) extends QueryElement:

  override def spec: String =
    s"${node1.fingerprint} $directionSpec ${node2.fingerprint}" +
      s"key: ${edgeKeyPredicates.map(_.spec).mkString(", ")}"

  def edgeKeyIs(s: String): EdgeDirectionPredicate =
    copy(edgeKeyPredicates =
      edgeKeyPredicates :+
        EdgeKeyPredicate(
          spec = "is",
          f = (k: String) => k == s
        )
    )

  def edgeKeyIsOneOf(ss: Set[String]): EdgeDirectionPredicate =
    copy(edgeKeyPredicates =
      edgeKeyPredicates :+
        EdgeKeyPredicate(
          spec = s"one of {${ss.toVector.sorted.mkString(",")}}",
          f = (k: String) => ss.contains(k)
        )
    )

  def edgeKeyMatches(regex: Regex): EdgeDirectionPredicate =
    copy(edgeKeyPredicates =
      edgeKeyPredicates :+
        EdgeKeyPredicate(
          spec = s"matches ${regex.pattern}",
          f = (k: String) => regex.matches(k)
        )
    )

case class EdgeKeyPredicate(
    override val spec: String,
    f: String => Boolean // Match the edge key
) extends QueryElement

