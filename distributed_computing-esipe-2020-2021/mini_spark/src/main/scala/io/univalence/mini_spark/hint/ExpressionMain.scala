package io.univalence.mini_spark.hint

object ExpressionMain {

  def main(args: Array[String]): Unit = {
    // represents 2*x + 1
    println(Add(Mul(Number(2), Variable), Number(1)))
    println(eval(Add(Mul(Number(2), Variable), Number(1)), 10)) // => 21
    println(
      simplify(Add(Mul(Number(2.0), Variable), Mul(Number(2.0), Variable)))
    )
  }

  // Based type for the ADT
  sealed trait Expression
  // Deriving types of the ADT
  case class Number(value: Double)                    extends Expression
  case class Add(left: Expression, right: Expression) extends Expression
  case class Mul(left: Expression, right: Expression) extends Expression
  case object Variable                                extends Expression

  def eval(expression: Expression, variableValue: Double): Double =
    expression match {
      case Variable      => variableValue
      case Number(value) => value
      case Add(left, right) =>
        eval(left, variableValue) + eval(right, variableValue)
      case Mul(left, right) =>
        eval(left, variableValue) * eval(right, variableValue)
    }

  def simplify(expression: Expression): Expression =
    expression match {
      case Add(Number(0.0), e)   => e
      case Add(e, Number(0.0))   => e
      case Mul(Number(1.0), e)   => e
      case Mul(e, Number(1.0))   => e
      case Add(e, f) if (e == f) => Mul(Number(2.0), e)
      // ...
      case e => e // default case
    }

}
