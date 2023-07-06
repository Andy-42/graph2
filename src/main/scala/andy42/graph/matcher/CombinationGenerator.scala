package andy42.graph.matcher

object CombinationGenerator:

  /** Generate all combinations that use one item from the outer vector.
    *
    * This is the generator method from StackOverflow via Micaela Martino's version (re-formatted)
    * https://stackoverflow.com/questions/23425930/generating-all-possible-combinations-from-a-listlistint-in-scala
    *
    * @param xs
    * A vector where each element is a vector of the possible values for that element of the outer vector.
    * @return
    * A vector of all the combinations that can be generated using one value from each of the vectors in xs.
    */
  def all[A](xs: Vector[Vector[A]]): Vector[Vector[A]] =
    xs.foldRight(Vector(Vector.empty[A])) { (next, combinations) =>
      for
        a <- next
        as <- combinations
      yield a +: as
    }

  def combinationsSingleUse[A](xs: Vector[Vector[A]]): Vector[Vector[A]] =
    xs.foldRight(Vector(Vector.empty[A])) { (next, combinations) =>
      for
        a <- next
        as <- combinations
        if !as.contains(a)
      yield a +: as
    }
