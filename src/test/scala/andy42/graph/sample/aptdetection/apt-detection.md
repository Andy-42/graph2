
This is example attempts to reproduce the behaviour of the [APT Detection](https://quine.io/recipes/apt-detection) from Quine.
Only the standing query logic is reproduced in this case.

The original Quine recipe include two input files [endpoint.json](https://recipes.quine.io/apt-detection/endpoint-json)
and [network.json](https://recipes.quine.io/apt-detection/network-json). 
I have implemented the ingestion of both of these files into the graph, but it appears
that only the `endpoint.json` data is relevant to this example.

- Added `EndpointEvent` label on those nodes (might not be necessary, revisit this)
- TODO: Use a different key on the events - reduces number of nodes to be matched


Here is the original standing query from the Quine APT Detection example:
```yaml
standingQueries:
  - pattern:
      type: Cypher
      query: >-
        MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2), 
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
        WHERE e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
        RETURN DISTINCT id(f) as fileId
    outputs:
      stolen-data:
        type: CypherQuery
        query: >-
          MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2), 
                (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
          WHERE id(f) = $that.data.fileId
            AND e1.type = "WRITE"
            AND e2.type = "READ"
            AND e3.type = "DELETE"
            AND e4.type = "SEND"
            AND e1.time < e2.time
            AND e2.time < e3.time
            AND e2.time < e4.time

          CREATE (e1)-[:NEXT]->(e2)-[:NEXT]->(e4)-[:NEXT]->(e3)

          WITH e1, e2, e3, e4, p1, p2, f, ip, "http://localhost:8080/#MATCH" + text.urlencode(" (e1),(e2),(e3),(e4),(p1),(p2),(f),(ip) WHERE id(p1)='"+strId(p1)+"' AND id(e1)='"+strId(e1)+"' AND id(f)='"+strId(f)+"' AND id(e2)='"+strId(e2)+"' AND id(p2)='"+strId(p2)+"' AND id(e3)='"+strId(e3)+"' AND id(e4)='"+strId(e4)+"' AND id(ip)='"+strId(ip)+"' RETURN e1, e2, e3, e4, p1, p2, f, ip") as URL
          RETURN URL
        andThen:
          type: PrintToStandardOut
```

Here is an graphical representation of this query:
```mermaid
flowchart TD
    p1[p1 label:Process] %% Added: Process label constraint
    p2[p2 label:Process] %% Added: Process label constraint
    e1[e1 type:WRITE\nlabel:EndpointEvent] %% Added: EndpointEvent label constraint
    e2[e2 type:READ\nlabel:EndpointEvent] %% Added: EndpointEvent label constraint
    e3[e3 type:DELETE\nlabel:EndpointEvent] %% Added: EndpointEvent label constraint
    e4[e4 type:SEND\nlabel:EndpointEvent] %% Added: EndpointEvent label constraint
    
    f[f] %% TODO: For output, expecting this node must have `time` and `data` properties
    ip[ip] %% TODO: For output, expecting this node to have an object property

    p1 --> |EVENT| e1
    e1 --> |EVENT| f
    e2 --> |EVENT| f
    p2 --> |EVENT| e2

    e3 --> |EVENT| f
    p2 --> |EVENT| e3
    p2 --> |EVENT| e4
    e4 --> |EVENT| ip
```

Currently, the only part of this that can't be represented in the existing scheme are the
time predicates:

```yaml
            AND e1.time < e2.time
            AND e2.time < e3.time
            AND e2.time < e4.time
```

This will need to be handled by another layer that is evaluated over a complete matched node set.
Also, the comparison logic here looks questionable - shouldn't this use `<=` since these events
could conceivably happen in rapid succession and could happen in within the time resolution of 1 ms.

# Edge predicate re-write

When an edge references a node that is unqualified, that predicate can be re-written
to move the predicate to the other node...