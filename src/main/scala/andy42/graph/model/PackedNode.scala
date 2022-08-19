package andy42.graph.model

type PackedNodeContents = Array[Byte]

trait PackedNode {

    def packed: PackedNodeContents
}

object PackedNode {
    val emptyPackedNodeContents = Array.emptyByteArray
}
