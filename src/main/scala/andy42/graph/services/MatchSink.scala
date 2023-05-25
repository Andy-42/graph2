package andy42.graph.services

import andy42.graph.matcher.{ResolvedMatches, SubgraphSpec}
import andy42.graph.model.EventTime
import zio.*

case class SubgraphMatchAtTime(time: EventTime, subgraphSpec: SubgraphSpec, nodeMatches: Vector[ResolvedMatches])

/** Accepts the matched subgraph specs.
  *
  * An implementation might write the matches to a persistent store (e.g., Kafka or a database), and it may also do some
  * work to remove duplicates.
  */
trait MatchSink:

  def offer(matches: SubgraphMatchAtTime): UIO[Unit]
