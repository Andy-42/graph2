package andy42.graph.model

import andy42.graph.model.*
import andy42.graph.services.EdgeReconciliationEvent
import andy42.graph.services.EdgeReconciliationEvent.apply
import org.msgpack.core.*
import zio.*

import java.io.IOException

enum Event extends Packable:
  case NodeRemoved
  
  case PropertyAdded(k: String, value: PropertyValueType)
  case PropertyRemoved(k: String)

  /**
    * When an Edge is added it is adding a _Near_ edge to the graph.
    * The graph will automatically create and add the corresponding _Far_ edge 
    * (the reverse half-edge) to the graph.
    */
  case EdgeAdded(edge: Edge)
  case EdgeRemoved(edge: Edge)
  
  case FarEdgeAdded(edge: Edge)
  case FarEdgeRemoved(edge: Edge)

  def isNearEdgeEvent: Boolean = this match
    case _: EdgeAdded | _: EdgeRemoved => true
    case _                             => false

  def isFarEdgeEvent: Boolean = this match
    case _: FarEdgeAdded | _: FarEdgeRemoved => true
    case _                                   => false

  override def pack(using packer: MessagePacker): Unit =
    this match
      case NodeRemoved =>
        packer.packInt(Event.nodeRemovedOrdinal)
        
      case PropertyAdded(k, value) =>
        packer.packInt(Event.propertyAddedOrdinal)
        packer.packString(k)
        PropertyValue.pack(value)

      case PropertyRemoved(k) =>
        packer.packInt(Event.propertyRemovedOrdinal)
        packer.packString(k)

      case EdgeAdded(edge) =>
        packer.packInt(Event.edgeAddedOrdinal)
        edge.pack

      case EdgeRemoved(edge) =>
        packer.packInt(Event.edgeRemovedOrdinal)
        edge.pack

      case FarEdgeAdded(edge) =>
        packer.packInt(Event.farEdgeAddedOrdinal)
        edge.pack

      case FarEdgeRemoved(edge) =>
        packer.packInt(Event.farEdgeRemovedOrdinal)
        edge.pack

object Event extends Unpackable[Event]:

  private val nodeRemovedOrdinal = 0
  private val propertyAddedOrdinal = 1
  private val propertyRemovedOrdinal = 2
  private val edgeAddedOrdinal = 3
  private val edgeRemovedOrdinal = 4
  private val farEdgeAddedOrdinal = 5
  private val farEdgeRemovedOrdinal = 6

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    unpacker.unpackInt() match
      case Event.nodeRemovedOrdinal     => ZIO.succeed(NodeRemoved)
      case Event.propertyAddedOrdinal   => unpackPropertyAdded
      case Event.propertyRemovedOrdinal => unpackPropertyRemoved
      case Event.edgeAddedOrdinal       => unpackEdgeAdded(isFar = false)
      case Event.edgeRemovedOrdinal     => unpackEdgeRemoved(isFar = false)
      case Event.farEdgeAddedOrdinal    => unpackEdgeAdded(isFar = true)
      case Event.farEdgeRemovedOrdinal  => unpackEdgeRemoved(isFar = true)

      case unexpectedEventType =>
        ZIO.fail(UnexpectedEventDiscriminator(unexpectedEventType))

  private def unpackPropertyAdded(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    UnpackSafely {
      for
        k <- ZIO.attempt(unpacker.unpackString())
        value <- PropertyValue.unpack
      yield PropertyAdded(k, value)
    }

  private def unpackPropertyRemoved(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    UnpackSafely {
      for k <- ZIO.attempt(unpacker.unpackString()) yield PropertyRemoved(k)
    }

  private def unpackEdgeAdded(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- if isFar then FarEdge.unpack else NearEdge.unpack
    yield if isFar then FarEdgeAdded(edge) else EdgeAdded(edge)

  private def unpackEdgeRemoved(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- if isFar then FarEdge.unpack else NearEdge.unpack
    yield if isFar then FarEdgeRemoved(edge) else EdgeRemoved(edge)

object Events extends CountedSeqPacker[Event]:

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[Event]] =
    UnpackSafely {
      given unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(packed)

      for
        length <- ZIO.attempt(unpacker.unpackInt())
        events <- UnpackOperations.unpackCountedToSeq(Event.unpack, length)
      yield events.toVector
    }
  
  def pack(events: Vector[Event]): Array[Byte] =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    packer.packInt(events.length)
    events.foreach(_.pack)
    packer.toByteArray
    