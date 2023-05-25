package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.{Graph, PersistenceFailure, TracingService}
import io.opentelemetry.api.trace.SpanKind
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

import scala.annotation.tailrec

type NodeSpecName = String
case class SpecNodeMatch(specName: NodeSpecName, id: NodeId):
  override def toString: String = s"$specName -> $id"

type ResolvedMatches = Set[SpecNodeMatch]

object ResolvedMatches:
  val empty: ResolvedMatches = Set.empty

trait Matcher:

  def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[ResolvedMatches]]

case class MatcherLive(subgraphSpec: SubgraphSpec, nodeSnapshotCache: MatcherSnapshotCache, tracing: Tracing)
    extends Matcher:

  /** Match the given nodes using each node spec in the subgraph spec as a starting point.
    * @param ids
    *   The ids of the nodes to be matched.
    * @return
    *   The successfully resolved matches between the given nodes and the nodes in the spec. This result may contain
    *   duplicate matches.
    */
  override def matchNodes(ids: Vector[NodeId]): NodeIO[Vector[ResolvedMatches]] =
    import tracing.aspects.*
    (
      for
        _ <- tracing.setAttribute("ids.length", ids.length)
        _ <- tracing.setAttribute("spec.node.length", subgraphSpec.allUniqueNodeSpecs.length)
        snapshots <- ZIO.foreachPar(ids)(nodeSnapshotCache.get) @@ span("fetch initial snapshots")
        potentialMatches <- ZIO.succeed(shallowMatchesInCrossProduct(ids, snapshots)) @@ span(
          "shallow matches in cross product"
        )
        resolvedMatches <- ZIO.foreachPar(potentialMatches) { (resolvedMatch, dependencies) =>
          prove(Set(resolvedMatch), dependencies)
        } @@ span("resolve potential matches")
      yield resolvedMatches.flatten
    ) @@ root("match nodes", SpanKind.SERVER)

  /** Create an initial match for each spec-node pair that shallow matches.
    * @param ids
    *   The ids of the nodes to be evaluated.
    * @param snapshots
    *   The corresponding node snapshot for node id.
    * @return
    *   An initial match, where the key is the spec-node that shallow matched, and the value is the node-id pair
    *   dependencies that must also match for this spec-node pair to be proven true.
    */
  def shallowMatchesInCrossProduct(
      ids: Vector[NodeId],
      snapshots: Vector[NodeSnapshot]
  ): Vector[(SpecNodeMatch, Dependencies)] =
    for
      (id, snapshot) <- ids.zip(snapshots)
      nodeSpec <- subgraphSpec.allUniqueNodeSpecs
      dependencies <- nodeSpec.shallowMatchNode(using snapshot)
      if dependencies.nonEmpty // empty indicates a failed match
      specNodeMatch = SpecNodeMatch(nodeSpec.name, id)
    yield specNodeMatch -> dependencies.filter(_ != specNodeMatch)

  /** Expand the proof for a node by traversing its unproven dependencies.
    *
    * The node-id keys matches are tested using shallow matching.
    *
    * Any failed matches have their dependencies removed from resolved matches. That dependency results in an invalid
    * dependency (i.e., one or more edges now has zero matches), then that entry is removed from resolved matches in the
    * next recursion. If resolved matches becomes empty, then the original spec-node being proven has failed to be
    * proven, so the overall match fails.
    *
    * If the match has not failed, then the proven spec-node pairs are added to resolvedMatches. This may add new
    * unproven matches that are proven on the next recursion.
    *
    * @param resolvedMatches
    *   The set of all spec-node pairs that have been resolve (or presumed resolved) at this point.
    * @param dependencies
    *   The spec-node pairs that must be proven on this step for the resolvedMatches to be true.
    * @return
    *   All the possible resolved matches. More than one possible solution may be possible if an edge spec matches more
    *   than one edge.
    */
  def prove(
      resolvedMatches: ResolvedMatches,
      dependencies: Vector[SpecNodeMatch]
  ): NodeIO[Vector[ResolvedMatches]] =
    require(dependencies.forall(!resolvedMatches.contains(_)))
    import tracing.aspects.*

    if dependencies.isEmpty then ZIO.succeed(Vector(resolvedMatches))
    else
      (
        for
          _ <- tracing.setAttribute("resolvedMatches.size", resolvedMatches.size)
          _ <- tracing.setAttribute("dependencies.length", dependencies.length)
          _ <- tracing.setAttribute("dependencies", dependencies.map(_.toString))

          snapshots <- nodeSnapshotCache.fetchSnapshots(dependencies.map(_.id)) @@ span(
            "fetch dependencies snapshots"
          )
          withDependenciesResolved <- resolveDependencies(resolvedMatches, dependencies, snapshots) @@ span(
            "resolve dependencies"
          )
        yield withDependenciesResolved
      ) @@ span("prove")

  def resolveDependencies(
      resolvedMatches: ResolvedMatches,
      dependencies: Vector[SpecNodeMatch],
      snapshots: Vector[NodeSnapshot]
  ): NodeIO[Vector[ResolvedMatches]] =

    // Shallow matching each dependency-snapshot produces a Vector[Vector[NodeSpecMatch]] where the
    // the outer vector represents a node, and the inner vector represents all the matches for each
    // edge. If an inner vector is empty, then that edge spec did not match any edges; but if the
    // edge spec matched multiple nodes, then there are multiple combinations that satisfy the node constraints.

    val dependenciesMatchedToNodes: Vector[Vector[Dependencies]] = dependencies
      .zip(snapshots)
      .map((dependency, snapshot) => subgraphSpec.nameToNodeSpec(dependency.specName).shallowMatchNode(using snapshot))

    // Applying the combination generator to it produces another Vector[Vector[NodeSpecMatch]],
    // but in this case, the other vector represents a possible resolution, and each element in the
    // inner vector represents the dependent node that must be matched for this combination to succeed.
    val matchCombinations: Vector[Vector[Dependencies]] = generator(dependenciesMatchedToNodes)

    if matchCombinations.isEmpty then tracing.addEvent("prove fails") *> ZIO.succeed(Vector.empty)
    else
      val nextResolvedMatches: ResolvedMatches = resolvedMatches ++ dependencies

      ZIO
        .foreachPar(matchCombinations) { combination =>
          val unresolvedDependencies = combination.flatten.distinct.filter(!nextResolvedMatches.contains(_))

          if unresolvedDependencies.isEmpty then
            tracing.addEvent("prove succeeds with one resolved match") *> ZIO.succeed(Vector(nextResolvedMatches))
          else
            tracing.addEvent(s"prove continues with ${unresolvedDependencies.length} unresolved dependencies") *>
              prove(nextResolvedMatches, unresolvedDependencies)

        }
        .map(_.flatten)

  /** Generate combinations.
    *
    * This is the method from StackOverflow via Micaela Martino's version (re-formatted)
    * https://stackoverflow.com/questions/23425930/generating-all-possible-combinations-from-a-listlistint-in-scala
    *
    * @param xs
    *   A list of lists of A
    * @return
    *   A list of all the combinations that can be generated using one value from each of the lists.
    */
  def generator[A](xs: Vector[Vector[A]]): Vector[Vector[A]] =
    xs.foldRight(Vector(Vector.empty[A])) { (next, combinations) =>
      for
        a <- next
        as <- combinations
      yield a +: as
    }

  type Dependencies = Vector[SpecNodeMatch]

  extension (nodeSpec: NodeSpec)
    def shallowMatchNode(using snapshot: NodeSnapshot): Vector[Dependencies] =
      if !nodeSpec.allNodePredicatesMatch then Vector.empty
      else
        // All the matches for each edge.
        // Outer: edge spec; Inner: all edges in the node that shallow match the spec
        generator(
          subgraphSpec
            .outgoingEdges(nodeSpec.name)
            .toVector // TODO: Change SubgraphSpec.outgoingEdges to Vector representation?
            .map(_.shallowMatchEdges(using snapshot))
        )

  extension (edgeSpec: EdgeSpec)
    def shallowMatchEdges(using snapshot: NodeSnapshot): Vector[SpecNodeMatch] =
      snapshot.edges.view.collect {
        case edge if edgeSpec.isShallowMatch(edge) => SpecNodeMatch(edgeSpec.direction.to.name, edge.other)
      }.toVector

object Matcher:

  def make(
      time: EventTime,
      graph: Graph,
      tracing: Tracing,
      subgraphSpec: SubgraphSpec,
      nodes: Vector[Node]
  ): UIO[MatcherLive] =
    for
      nodeCache <- MatcherNodeCache.make(graph, nodes)
      dataViewCache <- MatcherSnapshotCache.make(time, nodeCache, tracing)
    yield MatcherLive(subgraphSpec, dataViewCache, tracing)
