package andy42.graph.model

import zio.Task

object EventHasEffectOps:

  def filterHasEffect(events: Vector[Event], nodeState: NodeStateAtTime): Vector[Event] =
    if !events.exists(hasEffect(_, nodeState)) then events
    else events.filter(hasEffect(_, nodeState))

  def hasEffect(event: Event, nodeState: NodeStateAtTime): Boolean =
    event match
      case Event.NodeRemoved =>
        nodeState.properties.nonEmpty || nodeState.edges.nonEmpty

      case Event.PropertyAdded(k, value) =>
        nodeState.properties.get(k).fold(true)(_ == value)

      case Event.PropertyRemoved(k) =>
        nodeState.properties.contains(k)

      case Event.EdgeAdded(edge) =>
        !nodeState.edges.contains(edge)

      case Event.FarEdgeAdded(edge) =>
        !nodeState.edges.contains(edge)

      case Event.EdgeRemoved(edge) =>
        nodeState.edges.contains(edge)

      case Event.FarEdgeRemoved(edge) =>
        nodeState.edges.contains(edge)
