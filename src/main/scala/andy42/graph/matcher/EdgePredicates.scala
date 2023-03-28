package andy42.graph.matcher

import andy42.graph.model.EdgeDirection
import scala.util.hashing.MurmurHash3
import andy42.graph.model.NodeId
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

object EdgeDirectionPredicates:

  val isDirected = EdgeDirectionPredicate(
    spec = "-?-",
    f = (ed: EdgeDirection) => ed != EdgeDirection.Undirected
  )

  val isDirectedOut = EdgeDirectionPredicate(
    spec = "-->",
    f = (ed: EdgeDirection) => ed == EdgeDirection.Outgoing
  )

  val isDirectedIn = EdgeDirectionPredicate(
    spec = "<--",
    (ed: EdgeDirection) => ed == EdgeDirection.Incoming
  )

  val isUnDirected = EdgeDirectionPredicate(
    spec = "---",
    (ed: EdgeDirection) => ed == EdgeDirection.Undirected
  )

  val any = EdgeDirectionPredicate(
    spec = "-*-",
    f = (_: EdgeDirection) => true
  )

object EdgeKeyPredicates:

  val any: EdgeKeyPredicate =
    EdgeKeyPredicate(
      spec = "any",
      f = (_: String) => true
    )

  def is(s: String): EdgeKeyPredicate =
    EdgeKeyPredicate(
      spec = "is",
      f = (k: String) => k == s
    )

  def oneOf(ss: Set[String]): EdgeKeyPredicate =
    EdgeKeyPredicate(
      spec = s"one of {${ss.toVector.sorted.mkString(",")}}",
      f = (k: String) => ss.contains(k)
    )

  def matches(regex: Regex): EdgeKeyPredicate =
    EdgeKeyPredicate(
      spec = s"matches ${regex.pattern}",
      f = (k: String) => regex.matches(k)
    )

  // TODO: Do we want isReflexive? That can be pushed down to node predicates.
