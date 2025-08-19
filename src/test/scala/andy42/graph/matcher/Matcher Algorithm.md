# Matcher Requirements

The `matchNodes` method is called with a `Vector[NodeId]` that is all the node `ids` that changed in some
`Graph.append`.
The `matchNodes` matches those nodes against the `SubgraphSpec` it is configured with.

The design goal of the algorithm are:

* For every combination of nodes that satisfy the constraints of the `SubgraphSpec` at any given event time,
  a match event is generated. This is addressing completeness - we find all the matches.
* Matches can be generated within the scope of the fiber that is calling the `Graph.append` API.

For example, we could construct a program that successful completion of a `Graph.append` would mean that any
matches are known to have been sent to the output stream. A security monitoring application might require this
level of immediate matching.

Performance is an issue. If this is processing a live stream, then the matcher processing must be at least fast
enough to keep up with processing the stream. In reality, it has to be able to go faster than the stream in order to
allow for restarting.

# Matcher Algorithm

Given a SubgraphSpec such as the following (note that edges are unnamed, but this diagram shows the names):

```mermaid
flowchart TD
    a
    b
    c

    a --> b --> c
 ```

And four nodes in the group of mutated nodes: a, b, c, d. We do a shallow match of the nodes and get the following
matches:

```scala

Vector(
  Vector(
    "p1" -> "a"
  )
  
)
```