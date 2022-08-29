package andy42.graph.cache

import andy42.graph.model._
import zio._

object PersistenceConversions {

  def toEventsAtTime(
      graphHistory: GraphHistory
  ): IO[UnpackFailure, EventsAtTime] =
    for {
      events <- Events.unpack(graphHistory.events)
    } yield EventsAtTime(
      eventTime = graphHistory.eventTime,
      sequence = graphHistory.sequence,
      events = events
    )

  def toGraphHistory(id: NodeId, eventsAtTime: EventsAtTime): GraphHistory =
    GraphHistory(
      id = id,
      eventTime = eventsAtTime.eventTime,
      sequence = eventsAtTime.sequence,
      events = Events.pack(eventsAtTime.events)
    )
}
