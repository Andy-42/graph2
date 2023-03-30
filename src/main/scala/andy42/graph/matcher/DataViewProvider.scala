package andy42.graph.matcher

import andy42.graph.model.{NodeHistory, NodeIO, NodeSnapshot}

trait DataViewProvider:
  def history: NodeIO[NodeHistory]
  def snapshot: NodeIO[NodeSnapshot]
  
