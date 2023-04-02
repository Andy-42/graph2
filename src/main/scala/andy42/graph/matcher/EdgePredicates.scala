package andy42.graph.matcher

import andy42.graph.model.EdgeDirection

import scala.util.matching.Regex

/** Create a new edge specification.
  */
object EdgeSpecs:

  def directedEdge(from: NodeSpec, to: NodeSpec): EdgeSpec =
    EdgeSpec(from, to, (ed: EdgeDirection) => ed != EdgeDirection.Outgoing)

/** These extensions add an edge predicate to an EdgeSpec.
  */
extension (edgeSpec: EdgeSpec)

  def edgeKeyIs(s: String): EdgeSpec =
    edgeSpec.withKeyPredicate((k: String) => k == s)

  def edgeKeyIsOneOf(ss: Set[String]): EdgeSpec =
    edgeSpec.withKeyPredicate((k: String) => ss.contains(k))

  def edgeKeyMatches(regex: Regex): EdgeSpec =
    edgeSpec.withKeyPredicate((k: String) => regex.matches(k))
