package andy42.graph.cache

import andy42.graph.model.EventsAtTime.apply

import andy42.graph.model.NodeId
import andy42.graph.model.EventsAtTime

import zio.UIO

trait EdgeSynchronization {

  /** Ensure any far edges are synchronized. When an edge is set, a
    * [[NearEdgeEvent]] that represents a half-edge that is appended to the (near) node.
    * This service will eventually append a [[FarEdgeEvent]] to the other (far) node that corresponds
    * to the other half-edge.
    * 
    * Propagating the edges to the far nodes is necessarily asynchronous and eventually consistent
    * due to the lock-free graph implementation. If a synchronized graph was used, it would be easy to
    * construct a scenario where deadlock is possible. Since the we only have eventual consistency,
    * the EdgeSynchronization implementation should monitor (and perhaps repair) edge consistency.
    */
  def nearEdgesSet(id: NodeId, eventsAtTime: EventsAtTime): UIO[Unit]
}
