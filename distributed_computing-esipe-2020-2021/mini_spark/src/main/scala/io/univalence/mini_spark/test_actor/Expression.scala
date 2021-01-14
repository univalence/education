package io.univalence.mini_spark.test_actor

sealed trait Expression

case class Const(value: Double)                     extends Expression
case class Add(left: Expression, right: Expression) extends Expression
case class Mul(left: Expression, right: Expression) extends Expression
case object Var                                     extends Expression

object ExpressionMain {

  def eval(expression: Expression, value: Double): Double =
    expression match {
      case Var       => value
      case Const(c)  => c
      case Add(l, r) => eval(l, value) + eval(r, value)
      case Mul(l, r) => eval(l, value) * eval(r, value)
    }

  def simplify(expression: Expression): Expression =
    expression match {
      case Add(e, Const(0.0)) => e
      case Add(Const(0.0), e) => e
      case Mul(e, Const(1.0)) => e
      case Mul(Const(1.0), e) => e
      case Mul(_, Const(0.0)) => Const(0.0)
      case Mul(Const(0.0), _) => Const(0.0)

      case Add(l, r) => Add(simplify(l), simplify(r))

      case e => e
    }

  def main(args: Array[String]): Unit = {
    // 2 * x + 0
    val expression = Add(Mul(Const(2.0), Var), Const(0.0))

    println(expression)
    println(simplify(expression))
  }
}
