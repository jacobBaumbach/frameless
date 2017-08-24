package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class AggregatorsTest extends TypedDatasetSuite {
  test("typedaggregator") {
    def prop[A: TypedEncoder, B: TypedEncoder, O: TypedEncoder](
      zeroT: B,
      reduceT: (B, A) => B,
      mergeT: (B, B) => B,
      finishT: B => O,
      data: Vector[X1[A]]
    )(implicit ex1: TypedEncoder[X1[A]]): Prop = {

      val aggregator: TypedAggregator[X1[A], A, B, O] =
        new TypedAggregator[X1[A], A, B, O]{
          override def zero = zeroT
          override def reduce(b: B, a: A): B = reduceT(b, a)
          override def merge(b1: B, b2: B): B = mergeT(b1, b2)
          override def finish(b: B): O = finishT(b)
        }

      val column: TypedColumn[X1[A], O] = aggregator.toColumn

      val dataset: TypedDataset[X1[A]] = TypedDataset.create(data)

      val aggregatorResult = dataset.select(column).collect().run()

      val dataResult = finishT(data.foldLeft(zeroT)((b, x) => reduceT(b, x.a)))

      aggregatorResult ?= List(dataResult)
    }

    check(forAll(prop[Int, Int, Int] _))
    check(forAll(prop[String, String, String] _))
  }
}
