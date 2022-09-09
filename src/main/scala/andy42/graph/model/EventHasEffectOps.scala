package andy42.graph.model

import zio.Task

object EventHasEffectOps:

  def filterHasEffect(events: Vector[Event], nodeSnapshot: NodeSnapshot): Vector[Event] =
    if !events.exists(hasEffect(_, nodeSnapshot)) then events
    else events.filter(hasEffect(_, nodeSnapshot))

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
