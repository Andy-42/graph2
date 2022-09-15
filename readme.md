This project is an exercise I am using to learn Scala 3 and ZIO 2.
The goal is to implement a framework for streaming graph analysis.
It uses an idea from [Quine](https://quine.io) to build a graph from a stream,
and to implement _standing queries_ that are run on the graph data as the graph changes.

The idea is that the graph consumes a stream (or multiple streams) of input cause the graph
state to change, and the standing queries pattern match the graph to produce a stream
of sub-graphs.

While the Quine implementation uses the Akka actor model, this implementation uses ZIO 2.
Where Quine/Akka uses the Actor mailbox to ensure that the graph evolves in a consistent way,
this implementation explores the idea of a graph datastore that is completely lock free.
This uses _software transactional memory_ as the primary synchronization mechanism.

This project is currently at a very early stage and largely incomplete. At this point, only
the core services for storing and mutating data is implemented. It lacks any sort of meaningful
unit tests (next item on the agenda).

Quine implements the Cypher query langage. My vision for this implementation is not to have
any sort of query language at all but just a simple Scala DSL to expression queries.

Most of what is implemented currently is the core `Graph` API (get and append), and the
caching layer. The cache is designed primarily to service the standing query infrastructure.

The other sigificiant area that has been explored is the eventual consistency model.
The actor-based implementation is subject to deadlock due to the synchronous updating of
the far half-edges. In this model, only the near half-edge is updated as part of an append,
and the far half-edge is done in an eventually-consistent way. The `EdgeReconciliationService`
tracks (in a memory-bounded and efficient way) that all half-edges are consistent.
The next item on the agenda is to write the unit tests that prove the soundness of this technique.

The Quine implementation is limited in how it interacts with a Kafka data source.
While it can source changes from Kafka, it does not implement any idea of backpressure or
ensure than the those changes have been consumed sucessfully by the graph.
This implementation is designed to provide accurate feedback to Kafka so that when an append
request sent to the graph is completed successfully:
* Any changes have been persisted to the graph state, and
* All standing queries that could be affected by the change have been evaluated, and any matches have been committed to the query output stream.

The goal is that a Kafka Consumer could send a window of changes to the graph, and once they are complete, it would commit the offset for that window.