package andy42.graph

import andy42.graph.model.Event
import andy42.graph.model.EventTime
import andy42.graph.model.Node
import andy42.graph.model.NodeId
import zio.*
import zio.stm.*

/*
 * This is the entry point to applying any change to a node.
 * While changes can be applied as though they happened at any point in time (and in any order),
 * all changes to a node must be serialized. Rather than using an actor system and serializing the
 * changes by that mechanism, this API forces changes to be serialize by tracking the node ids that
 * have changes currently in flight in a transactional set.
 */
trait NodeMutation {

  /** Append events at a point in time. This is the fundamental API that all
    * mutation events are based on.
    * 
    * TODO: Narrow failure type
    */
  def append(id: NodeId, atTime: EventTime, events: Vector[Event]): Task[Node]
}

case class NodeMutationLive(inFlight: TSet[NodeId]) extends NodeMutation {

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  private def withNodeMutationPermit(id: NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  def append(id: NodeId, atTime: EventTime, events: Vector[Event]): Task[Node] =
    ZIO.scoped {
      for {
        _ <- withNodeMutationPermit(id)
      } yield ???
    }
}

object NodeMutationLive {
  val layer: ULayer[NodeMutation] =
    ZLayer {
      for {
        inFlight <- TSet.empty[NodeId].commit
      } yield NodeMutationLive(inFlight)
    }
}
