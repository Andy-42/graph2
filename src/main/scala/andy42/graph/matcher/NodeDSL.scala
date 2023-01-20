package andy42.graph.matcher

import andy42.graph.model.*

import zio.*
import andy42.graph.services.PersistenceFailure

type NodeIO[T] = IO[UnpackFailure | PersistenceFailure, T]

type NodeMatch = Node => NodeIO[Boolean]

extension (snapshot: NodeSnapshot)
  def hasProperty(k: String): Boolean =
    snapshot.properties.contains(k)

extension (history: NodeHistory)
  def hasProperty(k: String): Boolean =
    history.exists(_.events.exists {
      case Event.PropertyAdded(k_, _) => true
      case _                          => false
    })

  def propertySetMoreThanNTimes(k: String, n: Int): Boolean =
    history.flatMap(_.events).count {
      case Event.PropertyAdded(k, _) => true
      case _                         => false
    } > n

extension (edge: Edge)
  // def isFarEdge: Boolean = ??? // TODO: Near/far is not preserved in snapshot
  // def isNearEdge: Boolean = ???

  def isDirected: Boolean = edge.direction == EdgeDirection.Incoming || edge.direction == EdgeDirection.Outgoing
  def isDirectedOut: Boolean = edge.direction == EdgeDirection.Outgoing
  def isDirectedIn: Boolean = edge.direction == EdgeDirection.Incoming
  def isUnDirected: Boolean = !isDirected

  def isReflexive(id: NodeId): Boolean = id == edge.other

val nodeMatch1: NodeMatch =
  (node: Node) =>
    for snapshot <- node.current
    yield snapshot.hasProperty("a")

val nodeMatch2: NodeMatch =
  (node: Node) =>
    for
      history <- node.history
      current <- node.current
    yield !current.hasProperty("x") && history.propertySetMoreThanNTimes("b", 10)
