package fr.universite_gustave_eiffel.esipe.spark_course.course

import scala.annotation.tailrec

// ADT (Algebraic Data Type)
// (en compilation : AST - Abstract Syntax Tree)
sealed trait Expression
case object Variable                                 extends Expression
case class Constant(value: Double)                   extends Expression
case class Add(left: Expression, right: Expression)  extends Expression
case class Mult(left: Expression, right: Expression) extends Expression

object ExpressionMain {

  // Evaluate an expression for a given value for the variable
  def eval(expression: Expression, value: Double): Double =
    expression match {
      case Variable    => value
      case Constant(v) => v
      case Add(l, r)   => eval(l, value) + eval(r, value)
      case Mult(l, r)  => eval(l, value) * eval(r, value)
    }

  // simplify an expression
  def simplify(expression: Expression): Option[Expression] = {
    def simplify_(expression: Expression): Expression =
      expression match {
        case Mult(_, Constant(0))           => Constant(0)
        case Mult(Constant(0), _)           => Constant(0)
        case Mult(Constant(1), e)           => simplify_(e)
        case Mult(e, Constant(1))           => simplify_(e)
        case Add(e, Constant(0))            => simplify_(e)
        case Add(Constant(0), e)            => simplify_(e)
        case Add(Constant(n), Constant(m))  => Constant(n + m)
        case Mult(Constant(n), Constant(m)) => Constant(n * m)
        case Add(l, r)                      => Add(simplify_(l), simplify_(r))
        case Mult(l, r)                     => Mult(simplify_(l), simplify_(r))
        case _                              => expression
      }

    fixedPoint(simplify_)(expression)
  }

  /**
   * Get the fixed point of a function for a given value.
   *
   * The fixed point is the value x, such that `f(x) = x`.
   *
   * @param f function for which to find the fixed point
   * @param x0 value to start with
   * @param limit maximum number of iteration
   * @tparam A type of the value to find
   * @return the fixed of the function for the given value or None if the limit is 0
   */
  @tailrec
  def fixedPoint[A](f: A => A)(x0: A, limit: Int = 1000): Option[A] =
    if (limit == 0) None
    else {
      val x = f(x0)
      if (x == x0) Some(x)
      else fixedPoint(f)(x, limit - 1)
    }

  def main(args: Array[String]): Unit = {
    // 2 * x + 1
    val expression = Add(Mult(Constant(2), Variable), Constant(1))

    println(expression)
    println(eval(expression, 2))

    // TEST simplify
    println(simplify(Mult(Constant(0), Variable)))
    println(simplify(Mult(Constant(1), Mult(Constant(0), Variable))))
    println(simplify(Add(Constant(1), Constant(-1))))
    println(simplify(Mult(Constant(1), Mult(Add(Constant(1), Constant(-1)), Variable))))
  }
}
