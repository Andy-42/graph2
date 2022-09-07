package andy42.graph.model

import zio.Task

object EventHasEffectOps {

  def filterHasEffect(events: Vector[Event], nodeState: NodeStateAtTime): Vector[Event] =
    if !events.exists(hasEffect(_, nodeState)) then events
    else events.filter(hasEffect(_, nodeState))

  def hasEffect(event: Event, nodeState: NodeStateAtTime): Boolean =
    event match
      case NodeRemoved =>
        nodeState.properties.nonEmpty || nodeState.edges.nonEmpty

      case PropertyAdded(k, value) =>
        nodeState.properties.get(k).fold(true)(_ == value)

      case PropertyRemoved(k) =>
        nodeState.properties.contains(k)

      case EdgeAdded(edge) =>
        !nodeState.edges.contains(edge)

      case FarEdgeAdded(edge) =>
        !nodeState.edges.contains(edge)

      case EdgeRemoved(edge) =>
        nodeState.edges.contains(edge)

      case FarEdgeRemoved(edge) =>
        nodeState.edges.contains(edge)
}
