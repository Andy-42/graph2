package andy42.graph.matcher

import andy42.graph.model.*

/**
 * Predicates that can be used within a NodeSpec to constrain the node matching.
 */
object NodePredicates:

  def hasProperty(k: String): NodeSnapshot ?=> NodePredicate =
    summon[NodeSnapshot].properties.contains(k)

  def hasProperty(k: String, v: PropertyValueType): NodeSnapshot ?=> NodePredicate =
    summon[NodeSnapshot].properties.get(k).contains(v)

  def isLabeled(k: String): NodeSnapshot ?=> NodePredicate =
    summon[NodeSnapshot].properties.get(k).contains(())

  def doesNotHaveProperty(k: String): NodeSnapshot ?=> NodePredicate =
    !summon[NodeSnapshot].properties.contains(k)
