package andy42.graph.model

object TestNode:
  def apply(lsb: Int): Node = Node.empty(NodeId(0L, lsb.toLong))