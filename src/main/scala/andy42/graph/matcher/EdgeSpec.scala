package andy42.graph.matcher

import andy42.graph.model.EdgeDirection
import scala.util.hashing.MurmurHash3
import andy42.graph.model.NodeId
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

import andy42.graph.matcher.NodeSpecId
case class EdgeSpec(description: String, predicates: Vector[EdgePredicate])

trait EdgePredicate:
  val description: String
  val fingerprint: Int

case class EdgeDirectionPredicate(description: String, f: EdgeDirection => Boolean, fingerprint: Int) extends EdgePredicate

object EdgeDirectionPredicate:

  def apply(description: String, f: EdgeDirection => Boolean): EdgeDirectionPredicate =
    EdgeDirectionPredicate(
      description = description,
      fingerprint = MurmurHash3.productHash(("edge direction predicate", description)),
      f = f
    )

  val isDirected = EdgeDirectionPredicate(
    description = "is directed",
    f = (ed: EdgeDirection) => ed != EdgeDirection.Undirected
  )

  val isDirectedOut = EdgeDirectionPredicate(
    description = "is directed out",
    f = (ed: EdgeDirection) => ed == EdgeDirection.Outgoing
  )

  val isDirectedIn = EdgeDirectionPredicate(
    description = "is directed in",
    (ed: EdgeDirection) => ed == EdgeDirection.Incoming
  )

  val isUnDirected = EdgeDirectionPredicate(
    description = "is undirected",
    (ed: EdgeDirection) => ed == EdgeDirection.Undirected
  )

  val any = EdgeDirectionPredicate(
    description = "any direction",
    f = (_: EdgeDirection) => true
  )


case class EdgeKeyPredicate(
    override val description: String,
    override val fingerprint: Int,
    f: String => Boolean // Match the edge key
) extends EdgePredicate

object EdgeKeyPredicate:

  def apply(description: String, f: String => Boolean): EdgeKeyPredicate =
    EdgeKeyPredicate(
      description = description,
      fingerprint = MurmurHash3.productHash(("key predicate", description)),
      f = f
    )

  val any: EdgeKeyPredicate =
    EdgeKeyPredicate(description = "any", f = (_: String) => true)

  def is(s: String): EdgeKeyPredicate =
    EdgeKeyPredicate(description = "is", f = (k: String) => k == s)

  def oneOf(ss: Set[String]): EdgeKeyPredicate =
    EdgeKeyPredicate(description = s"one of {${ss.toVector.sorted.mkString(",")}}", f = (k: String) => ss.contains(k))

  def matches(regex: Regex): EdgeKeyPredicate =
    EdgeKeyPredicate(description = s"matches ${regex.pattern}", f = (k: String) => regex.matches(k))

  // TODO: Want isReflexive, but need to do that on EdgePredicate since you need to compare IDs  

//case class EdgePredicate(
//    nearNode: NodeSpecId,
//    farNode: NodeSpecId,
//    direction: EdgeDirectionPredicate,
//    key: EdgeKeyPredicate
//)


case class EdgeSpecBuilder(
    predicates: ListBuffer[EdgePredicate] = ListBuffer.empty
):
  def build: Vector[EdgePredicate] = predicates.toVector
  def stage(predicate: EdgePredicate): Unit = predicates.append(predicate)

type EdgeSpecX = EdgeSpecBuilder ?=> Unit // TODO: Rename

def edge(description: String)(body: EdgeSpecBuilder ?=> EdgePredicate): EdgeSpec =
  given builder: EdgeSpecBuilder = EdgeSpecBuilder()
  body
  EdgeSpec(description = description, predicates = builder.build)