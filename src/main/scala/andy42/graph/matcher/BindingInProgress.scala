package andy42.graph.matcher

import andy42.graph.model.*

/** This is produced when a `NodeSpec` is matched against a node. A ShallowMatch contains all the possible bindings for
  * each edge spec.
  */
case class BindingInProgress(resolved: SpecNodeBindings, unresolved: SpecNodeBindings):
  require(if resolved.isEmpty then unresolved.isEmpty else true)

  def isEmpty: Boolean = resolved.isEmpty

  def isFullyResolved: Boolean = unresolved.isEmpty

  def isPartiallyResolved: Boolean = unresolved.nonEmpty

object BindingInProgress:

  final def combineUnresolved(resolved: SpecNodeBindings, unresolved: Seq[SpecNodeBinding]): Option[SpecNodeBindings] =
    unresolved.foldLeft(Option(Map.empty[NodeSpecName, NodeId])) { (maybeCombinedUnresolved, unresolvedBinding) =>
      maybeCombinedUnresolved.flatMap { combinedUnresolved =>

        val (unresolvedNodeSpecName, unresolvedNodeId) = unresolvedBinding

        // Is this binding compatible with the resolved bindings?
        resolved.get(unresolvedNodeSpecName) match
          case Some(id) =>
            if id != unresolvedNodeId then None else maybeCombinedUnresolved
          case None =>
            // Is this binding compatible with the unresolved that have been accumulated so far?
            combinedUnresolved.get(unresolvedNodeSpecName) match
              case Some(id) =>
                if id != unresolvedNodeId then None else maybeCombinedUnresolved
              case None =>
                Some(combinedUnresolved + unresolvedBinding)
      }
    }

  def combine(a: SpecNodeBindings, b: Seq[SpecNodeBinding]): Option[SpecNodeBindings] =
    b.foldLeft(Option(a)) { (r, binding) =>
      r.flatMap { bindings =>
        bindings.get(binding._1) match
          case Some(id) if id != binding._2 => None
          case Some(id)                     => r
          case _                            => Some(bindings + binding)
      }
    }

  def combine(a: Seq[SpecNodeBindings]): Option[SpecNodeBindings] =
    if a.isEmpty then Some(Map.empty)
    else if a.length == 1 then Some(a.head)
    else combine(a.head, a.tail.flatten)

  def combineBindingsInProgress(a: Seq[BindingInProgress]): Option[BindingInProgress] =
    combine(a.map(_.resolved))
      .flatMap { combinedResolved =>
        combineUnresolved(combinedResolved, a.flatMap(_.unresolved))
          .map(BindingInProgress(combinedResolved, _))
      }
