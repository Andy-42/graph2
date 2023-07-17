package andy42.graph.matcher

import andy42.graph.matcher.EdgeSpecs.directedEdge
import andy42.graph.model.*
import zio.Scope
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault}

/** This spec shows how combinations are generated and filtered in the matcher algorithm.
  */
object MatcherCombinationsSpec extends ZIOSpecDefault:

  val eventTime = 0; val sequence = 0 // all time is the the same point in this spec

  val nodeId1: NodeId = NodeId(1, 0)
  val nodeId2: NodeId = NodeId(2, 0)
  val nodeId3: NodeId = NodeId(3, 0)
  val nodeId4: NodeId = NodeId(4, 0)

  val node1: Node = Node.fromHistory(
    id = nodeId1,
    history = Vector(
      EventsAtTime(
        time = eventTime,
        sequence = sequence,
        events = Vector(
          Event.PropertyAdded("p", 1L),
          Event.EdgeAdded(EdgeImpl("e1", nodeId2, EdgeDirection.Outgoing))
        )
      )
    )
  )
  val node2: Node = Node.fromHistory(
    id = nodeId2,
    history = Vector(
      EventsAtTime(
        time = eventTime,
        sequence = sequence,
        events = Vector(
          Event.PropertyAdded("p", 2L),
          Event.EdgeAdded(Edge("e1", nodeId1, EdgeDirection.Outgoing)),
          Event.EdgeAdded(Edge("e2", nodeId3, EdgeDirection.Outgoing)),
          Event.EdgeAdded(Edge("e2", nodeId4, EdgeDirection.Outgoing))
        )
      )
    )
  )
  val node3: Node = Node.fromHistory(
    id = nodeId3,
    history = Vector(
      EventsAtTime(
        time = eventTime,
        sequence = sequence,
        events = Vector(
          Event.PropertyAdded("p", 3L),
          Event.EdgeAdded(EdgeImpl("e2", nodeId2, EdgeDirection.Incoming))
        )
      )
    )
  )
  val node4: Node = Node.fromHistory(
    id = nodeId4,
    history = Vector(
      EventsAtTime(
        time = eventTime,
        sequence = sequence,
        events = Vector(
          Event.PropertyAdded("p", 3L),
          Event.EdgeAdded(EdgeImpl("e2", nodeId2, EdgeDirection.Incoming))
        )
      )
    )
  )

  val nodeSpecA: NodeSpec = node("a").hasProperty("p", 1L)
  val nodeSpecB: NodeSpec = node("b").hasProperty("p", 2L)
  val nodeSpecC: NodeSpec = node("c").hasProperty("p", 3L)

  val edgeSpecAtoB: EdgeSpec = directedEdge(from = nodeSpecA, to = nodeSpecB).edgeKeyIs("e1")
  val edgeSpecBtoC: EdgeSpec = directedEdge(from = nodeSpecB, to = nodeSpecC).edgeKeyIs("e2")

  val subgraphSpec: SubgraphSpec = subgraph("matcher combination spec")(edgeSpecAtoB, edgeSpecBtoC)

  // shallowMatchesForEachMutatedNode produces (mutation x nodeSpec) shallowMatches

  // The b x node2 match has two matches for the edge to "c".

  // The shallow match algorithm requires that every edge spec has at least one match,
  // or the resulting match vector is empty (i.e., no match).

  // If the match configuration requires that all mutations in the group are matched,
  // then this continues since for each node, there is at least one spec that has some matches.
  // Otherwise, an empty list of matches is returned.

  // Shallow matches calculated for each NodeSpec-Node pair

  val mutations: Seq[NodeId] = Vector(nodeId1, nodeId2, nodeId3)

  val allNodeSpecMutationPairs: Seq[SpecNodeBinding] =
    Seq(
      "a" -> nodeId1,
      "a" -> nodeId2,
      "a" -> nodeId3,
      "b" -> nodeId1,
      "b" -> nodeId2,
      "b" -> nodeId3,
      "c" -> nodeId1,
      "c" -> nodeId2,
      "c" -> nodeId3
    )

  // TODO: add a case where there is an extra edge to some other node?

  val shallowMatches: Seq[Vector[Vector[SpecNodeBinding]]] =
    Seq(
      Vector(Vector("b" -> nodeId2)), // nodeSpecA -> nodeId1
      Vector(), // nodeSpecA -> nodeId2
      Vector(), // nodeSpecA -> nodeId3

      Vector(), // nodeSpecB -> nodeId1
      Vector(Vector("a" -> nodeId1), Vector("c" -> nodeId3, "c" -> nodeId4)), // nodeSpecB -> nodeId2
      Vector(), // nodeSpecB -> nodeId3

      Vector(), // nodeSpecC -> nodeId1
      Vector(), // nodeSpecC -> nodeId2
      Vector(Vector("b" -> nodeId2)) // nodeSpecC -> nodeId3
    )

  val initialMatchIndividual: Seq[BindingInProgress] =
    Seq(
      BindingInProgress(resolved = Map("a" -> nodeId1), unresolved = Map("b" -> nodeId2)),
      BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId3)),
      BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId4)),
      BindingInProgress(resolved = Map("c" -> nodeId3), unresolved = Map("b" -> nodeId2))
    )

  // TODO: Show the rest of the matching for each of these cases

  // For the case where all mutations must match, gather all the matches for each nodeSpec.

  val bindingsForEachNodeSpecThatHasABinding: Seq[Seq[BindingInProgress]] =
    Seq(
      Seq(
        BindingInProgress(resolved = Map("a" -> nodeId1), unresolved = Map("b" -> nodeId2))
      ),
      Seq(
        BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId3)),
        BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId4))
      ),
      Seq(
        BindingInProgress(resolved = Map("c" -> nodeId3), unresolved = Map("b" -> nodeId2))
      )
    )

  // All the combinations that use one

  val combinationsUsingOneBindingForEachNodeSpec: Seq[Seq[BindingInProgress]] =
    Seq(
      Seq(
        BindingInProgress(resolved = Map("a" -> nodeId1), unresolved = Map("b" -> nodeId2)),
        BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId3)),
        BindingInProgress(resolved = Map("c" -> nodeId3), unresolved = Map("b" -> nodeId2))
      ),
      Seq(
        BindingInProgress(resolved = Map("a" -> nodeId1), unresolved = Map("b" -> nodeId2)),
        BindingInProgress(resolved = Map("b" -> nodeId2), unresolved = Map("a" -> nodeId1, "c" -> nodeId4)),
        BindingInProgress(resolved = Map("c" -> nodeId3), unresolved = Map("b" -> nodeId2))
      )
    )

  // Combining the first case results in a fully-resolved binding.
  // No further processing is needed, and the resolved field of that binding is offered to the MatchSink.
  // The second case is eliminated since there is a conflicting binding for node "c".

  // The edge of from nodeId2 -> nodeId4 is not relevant since matching all the mutations
  // implies that the mutations are a fully-connected group, so the first round only needs to
  // resolved edges that originate from one of the mutations.

  // Counter argument: What if adding these mutations would result in a solution through nodeId4?
  // Answer: It is not possible, since the groups represent an atomic unit of change, it would have
  // to be appended as part of a group and would be analyzed at that point.

  // Also note that in this case, the resolved binding references all the mutations, so it
  // satisfies the resolvedBindingExistsForEveryMutation

  val combined: Vector[BindingInProgress] =
    Vector(
      BindingInProgress(resolved = Map("a" -> nodeId1, "b" -> nodeId2, "c" -> nodeId3), unresolved = Map())
    )

  override def spec: Spec[TestEnvironment with Scope, Any] = ???
