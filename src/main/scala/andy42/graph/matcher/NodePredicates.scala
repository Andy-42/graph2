package andy42.graph.matcher

import andy42.graph.model.*

object NodePredicates:

  // These select the ambient NodeSnapshot alignment with time, but do not add a predicate to the builder

  def usingEventTime: NodeLocalSpecBuilder ?=> Unit =
    summon[NodeLocalSpecBuilder].snapshotSelector = SnapshotSelector.EventTime

  def usingNodeCurrent: NodeLocalSpecBuilder ?=> Unit =
    summon[NodeLocalSpecBuilder].snapshotSelector = SnapshotSelector.NodeCurrent

  def usingAtTime(time: EventTime): NodeLocalSpecBuilder ?=> Unit =
    summon[NodeLocalSpecBuilder].snapshotSelector = SnapshotSelector.AtTime(time)

  // These predicates let you supply your own implementation

  def snapshotFilter(spec: String, p: NodeSnapshot => Boolean): NodeLocalSpecBuilder ?=> Unit =
    val builder = summon[NodeLocalSpecBuilder]
    val selector = builder.snapshotSelector
    builder.stage(
      NodePredicate.Snapshot(
        selector = selector,
        spec = selector.makeSpec(spec),
        f = p
      )
    )

  def historyFilter(spec: String, p: NodeHistory => Boolean): NodeLocalSpecBuilder ?=> Unit =
    summon[NodeLocalSpecBuilder]
      .stage(NodePredicate.History(spec = s"history => $spec", f = p))

  // Convenience implementations for common operations

  def hasProperty(k: String): NodeLocalSpecBuilder ?=> Unit =
    snapshotFilter(
      spec = s"hasProperty($k)", 
      p = (ns: NodeSnapshot) => ns.properties.contains(k)
    )

  def hasProperty(k: String, v: PropertyValueType): NodeLocalSpecBuilder ?=> Unit =
    snapshotFilter(
      spec = s"hasPropertyWithValue($k,$v)",
      p = (ns: NodeSnapshot) => ns.properties.get(k) == Some(v)
    )

  def labeled(k: String): NodeLocalSpecBuilder ?=> Unit =
    snapshotFilter(
      spec = s"labeled($k)",
      p = (ns: NodeSnapshot) => ns.properties.get(k) == Some(())
    )

  def doesNotHaveProperty(k: String): NodeLocalSpecBuilder ?=> Unit =
    snapshotFilter(
      s"doesNotHaveProperty($k)", 
      (ns: NodeSnapshot) => !ns.properties.contains(k)
    )


// These are some examples of functions that can be used with historyFilter or snapshot filter
// (with the addition of a spec string) to produce a NodePredicate

object SomeOtherPredicateIdeas:
  import NodePredicates.{snapshotFilter, historyFilter}

  private def matchesPropertyAdded(k: String): Event => Boolean =
    (event: Event) =>
      event match {
        case Event.PropertyAdded(k, _) => true
        case _                         => false
      }

  def hasPropertySetAtAnyPointInHistory(k: String): NodeLocalSpecBuilder ?=> Unit =
    historyFilter(
      spec = s"hasPropertySetAtAnyPointInHistory($k)", 
      p = (ns: NodeHistory) => ns.exists(_.events.exists(matchesPropertyAdded(k)))
    )

  def hasPropertySetMoreThanNTimes(k: String, n: Int): NodeLocalSpecBuilder ?=> Unit =
    historyFilter(
      spec = s"hasPropertySetMoreThanNTimes($k,$n)",
      p = (ns: NodeHistory) => ns.foldLeft(0) { case (z, eventsAtTime) =>
        z + eventsAtTime.events.count(matchesPropertyAdded(k))
      } > n
    )
