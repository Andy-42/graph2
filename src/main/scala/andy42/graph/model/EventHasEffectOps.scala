package andy42.graph.model

import zio.Task

object EventHasEffectOps:

  /**
    * Remove any events that would have no effect on the given snapshot.
    * This is calculated in a way that does not copy `events` if there is no
    * change to the state. This avoids heap allocation pressure for this frequently
    * executed operation.
    */
  def filterHasEffect(events: Vector[Event], nodeSnapshot: NodeSnapshot): Vector[Event] =
    if !events.exists(hasEffect(_, nodeSnapshot)) then events
    else events.filter(hasEffect(_, nodeSnapshot))

  /**
    * Determine if an Event would have any effect if applied to a Node's history
    * given the collapesed node snapshot calculated for that point in time.
    */  
  def hasEffect(event: Event, nodeSnapshot: NodeSnapshot): Boolean =
    event match
      case Event.NodeRemoved =>
        nodeSnapshot.properties.nonEmpty || nodeSnapshot.edges.nonEmpty

      case Event.PropertyAdded(k, value) =>
        nodeSnapshot.properties.get(k).fold(true)(_ == value)

      case Event.PropertyRemoved(k) =>
        nodeSnapshot.properties.contains(k)

      case Event.EdgeAdded(edge) =>
        !nodeSnapshot.edges.contains(edge)

      case Event.FarEdgeAdded(edge) =>
        !nodeSnapshot.edges.contains(edge)

      case Event.EdgeRemoved(edge) =>
        nodeSnapshot.edges.contains(edge)

      case Event.FarEdgeRemoved(edge) =>
        nodeSnapshot.edges.contains(edge)
