package andy42.graph.services

import andy42.graph.model.*
import zio.*

import scala.collection.mutable

trait InputValidationFailure

case class InconsistentEvents(id: NodeId, event1: Event, event2: Event)

case class InconsistentEventsFailure(inconsistentEvents: Vector[InconsistentEvents]) extends InputValidationFailure

object NodeMutationCompletion:

  def completeAndValidate(changes: Vector[NodeMutationInput]): IO[InputValidationFailure, Vector[NodeMutationInput]] =
    checkConsistency(
      addMissingEdgeEvents(
        removeEmptyElements(
          consolidateIfNecessary(changes)
        )
      )
    )

  def consolidateIfNecessary(changes: Vector[NodeMutationInput]): Vector[NodeMutationInput] =
    val ids = changes.map(_.id).distinct
    if ids.length == changes.length then changes
    else ids.map(id => NodeMutationInput(id = id, events = changes.filter(_.id == id).map(_.events).reduce(_ ++ _)))

  def removeEmptyElements(changes: Vector[NodeMutationInput]): Vector[NodeMutationInput] =
    changes.filter(_.events.nonEmpty)

  def addMissingEdgeEvents(changes: Vector[NodeMutationInput]): Vector[NodeMutationInput] =

    def eventExists(id: NodeId, event: Event): Boolean =
      changes
        .find(_.id == id)
        .fold(false)(_.events.exists(_ == event))

    val edgeEventsToAdd: mutable.Map[NodeId, Vector[Event]] = mutable.Map.empty

    def addEdgeEvent(id: NodeId, event: Event): Unit =
      if !eventExists(id, event) then edgeEventsToAdd.put(id, edgeEventsToAdd.getOrElse(id, Vector.empty) :+ event)

    for
      change <- changes
      id = change.id
      event <- change.events
    do
      event match
        case Event.EdgeAdded(edge)   => addEdgeEvent(edge.other, Event.EdgeAdded(edge.reverse(id)))
        case Event.EdgeRemoved(edge) => addEdgeEvent(edge.other, Event.EdgeRemoved(edge.reverse(id)))
        case _                       =>

    if edgeEventsToAdd.isEmpty then changes
    else
      val idsToBeAdded = edgeEventsToAdd.collect { case (id, _) if !changes.exists(_.id == id) => id }
      val allChanges = changes ++ idsToBeAdded.map(id => NodeMutationInput(id, Vector.empty))
      allChanges.map { change =>
        edgeEventsToAdd.get(change.id).fold(change)(eventsToAdd => change.copy(events = change.events ++ eventsToAdd))
      }

  def checkConsistency(input: Vector[NodeMutationInput]): IO[InputValidationFailure, Vector[NodeMutationInput]] =

    def sameEdge(edge1: Edge, edge2: Edge): Boolean = edge1.other == edge2.other && edge1.k == edge2.k

    def eventsAreInconsistent(e1: Event, e2: Event): Boolean =
      e1 match
        case Event.PropertyAdded(k1, v1) =>
          e2 match
            case Event.PropertyAdded(k2, v2) => k1 == k2 && v1 != v2
            case Event.PropertyRemoved(k2)   => k1 == k2
            case _                           => false

        case Event.PropertyRemoved(k1) =>
          e2 match
            case Event.PropertyAdded(k2, _) => k1 == k2
            case _                          => false

        case Event.EdgeAdded(edge1) =>
          e2 match
            case Event.EdgeRemoved(edge2) => sameEdge(edge1, edge2)
            case _                        => false

        case Event.EdgeRemoved(edge1) =>
          e2 match
            case Event.EdgeAdded(edge2) => sameEdge(edge1, edge2)
            case _                      => false

        case Event.NodeRemoved =>
          e2 match
            case _: Event.EdgeAdded | _: Event.PropertyAdded => true
            case _                                           => false

    val inconsistentEvents =
      for
        inputForOneNode <- input
        events = inputForOneNode.events
        // Compare cross product - upper triangular excluding the diagonal so each non-self pair is compared once.
        i <- events.indices
        j <- events.indices
        if i > j
        event1 = events(i)
        event2 = events(j)
        if eventsAreInconsistent(event1, event2)
      yield InconsistentEvents(inputForOneNode.id, event1, event2)

    if inconsistentEvents.isEmpty then ZIO.succeed(input) else ZIO.fail(InconsistentEventsFailure(inconsistentEvents))
