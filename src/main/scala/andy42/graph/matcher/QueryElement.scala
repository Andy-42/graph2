package andy42.graph.matcher

import scala.util.hashing.MurmurHash3

type QueryElementFingerprint = Int

/**
 * All elements of the query DSL have a text representation and an integer fingerprint. 
 * 
 * @param spec
  *   Specification for the matcher - human readable and basis for the fingerprint.
  * @param fingerprint
  *   A more compact integer to (mostly) uniquely identify the node predicate logic.
  */
trait QueryElement:
  def spec: String
  def fingerprint: QueryElementFingerprint = MurmurHash3.stringHash(spec)
