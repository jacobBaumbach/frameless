package frameless

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator


abstract class TypedAggregator[T, I, B, O](
  implicit val bencoder: TypedEncoder[B],
  val oencoder: TypedEncoder[O]){

  def zero: B

  def reduce(b: B, a: I): B

  def merge(b1: B, b2: B): B

  def finish(b: B): O

  def bufferEncoder: Encoder[B] = TypedExpressionEncoder[B]

  def outputEncoder: Encoder[O] = TypedExpressionEncoder[O]

  def untyped(typedAggregator: TypedAggregator[T, I, B, O] = this): Aggregator[I, B, O] =
    new Aggregator[I, B, O]{
      override def zero = typedAggregator.zero
      override def reduce(b: B, a: I): B = typedAggregator.reduce(b, a)
      override def merge(b1: B, b2: B): B = typedAggregator.merge(b1, b2)
      override def finish(b: B): O = typedAggregator.finish(b)
      override def bufferEncoder: Encoder[B] = typedAggregator.bufferEncoder
      override def outputEncoder: Encoder[O] = typedAggregator.outputEncoder
    }

  def toColumn: TypedColumn[T, O] =
    new TypedColumn(this.untyped().toColumn)

}
