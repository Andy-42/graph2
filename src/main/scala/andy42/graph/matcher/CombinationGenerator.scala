package andy42.graph.matcher

object CombinationGenerator:

  /** Generate all combinations that use one item from the outer vector.
    *
    * This is the generator method from StackOverflow via Micaela Martino's version (re-formatted)
    * https://stackoverflow.com/questions/23425930/generating-all-possible-combinations-from-a-listlistint-in-scala
    *
    * @param xs
    *   A sequence where each element is a sequence of the possible values for that element of the outer sequence.
    * @return
    *   A sequence of all the combinations that can be generated using one value from each of the sequences in xs.
    */
  def generateCombinations[A](xs: Seq[Seq[A]]): Seq[Seq[A]] =
    xs.foldRight(Seq(Seq.empty[A])) { (next, combinations) =>
      for
        a <- next
        as <- combinations
      yield a +: as
    }

  type GeneratorInput[A] = Seq[Seq[A]]
