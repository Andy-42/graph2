package andy42.graph.matcher

import andy42.graph.model.*

object NodePredicates:

  // These select the ambient NodeSnapshot alignment with time, but do not add a predicate

  def usingEventTime: NodeLocalSpec = summon[NodeLocalSpecBuilder].snapshotSelector = SnapshotSelector.EventTime

  def usingNodeCurrent: NodeLocalSpec = summon[NodeLocalSpecBuilder].snapshotSelector = SnapshotSelector.NodeCurrent

  def usingAtTime(time: EventTime): NodeLocalSpec = summon[NodeLocalSpecBuilder].snapshotSelector =
    SnapshotSelector.AtTime(time)

  // These predicates let you supply your own implementation
  def snapshotFilter(p: NodeSnapshot => Boolean): NodeLocalSpec = summon[NodeLocalSpecBuilder]
    .snapshotFilter(p)

  def historyFilter(p: NodeHistory => Boolean): NodeLocalSpec = summon[NodeLocalSpecBuilder]
    .historyFilter(p)

  // Convenience implementations for common operations

  def hasProperty(k: String): NodeLocalSpec = summon[NodeLocalSpecBuilder]
    .snapshotFilter(_.properties.contains("p"))

  def doesNotHaveProperty(k: String): NodeLocalSpec = summon[NodeLocalSpecBuilder]
    .snapshotFilter(!_.properties.contains("p"))

object UtilitiesForOtherPredicates:

  def matchesPropertyAdded(k: String): Event => Boolean =
    (event: Event) =>
      event match {
        case Event.PropertyAdded(k, _) => true
        case _                         => false
      }

  // Haven't done any Edge predicates yet
  extension (edge: Edge)
    def isFarEdge: Boolean = edge.isInstanceOf[FarEdge]
    def isNearEdge: Boolean = edge.isInstanceOf[NearEdge]

    def isDirected: Boolean = edge.direction == EdgeDirection.Incoming || edge.direction == EdgeDirection.Outgoing
    def isDirectedOut: Boolean = edge.direction == EdgeDirection.Outgoing
    def isDirectedIn: Boolean = edge.direction == EdgeDirection.Incoming
    def isUnDirected: Boolean = !isDirected

    def isReflexive(id: NodeId): Boolean = id == edge.other

object SomeOtherPredicateIdeas:
  import UtilitiesForOtherPredicates.*

  def hasPropertySetAtAnyPointInHistory(k: String): NodeHistory => Boolean =
    _.exists(_.events.exists(matchesPropertyAdded(k)))

  def hasPropertySetMoreThanNTimes(k: String, n: Int): NodeHistory => Boolean =
    (nodeHistory: NodeHistory) =>
      nodeHistory.foldLeft(0) { case (z, eventsAtTime) =>
        z + eventsAtTime.events.count(matchesPropertyAdded(k))
      } > n
