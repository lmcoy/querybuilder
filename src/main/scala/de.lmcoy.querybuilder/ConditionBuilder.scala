package de.lmcoy.querybuilder

import de.lmcoy.querybuilder.ConditionBuilder.binderValueType
import de.lmcoy.querybuilder.QueryBuilder.SyntaxProvider
import scalikejdbc.{
  Binders,
  ConditionSQLBuilder,
  ParameterBinderFactory,
  ParameterBinderWithValue
}

class ConditionBuilder[A](implicit g: SyntaxProvider[A]) {

  type Transform[A] = ConditionSQLBuilder[A] => ConditionSQLBuilder[A]

  implicit val binderSQLType = new ParameterBinderFactory[SQLType] {
    def apply(value: SQLType): ParameterBinderWithValue = value match {
      case v: ValueType => binderValueType(v)
      case ColumnType(c) =>
        ParameterBinderFactory.sqlSyntaxParameterBinderFactory(
          g.column(c.field))
    }
  }

  private def binary(
      filter: BinaryFilter,
      initial: ConditionSQLBuilder[A]): ConditionSQLBuilder[A] = {

    filter match {
      case Eq(l, r) => initial.eq(g.column(l.field), r)
      case Gt(l, r) => initial.gt(g.column(l.field), r)
      case Ne(l, r) => initial.ne(g.column(l.field), r)
      case Ge(l, r) => initial.ge(g.column(l.field), r)
      case Le(l, r) => initial.le(g.column(l.field), r)
      case Lt(l, r) => initial.lt(g.column(l.field), r)
    }
  }

  private def unary(filter: UnaryFilter,
                    initial: ConditionSQLBuilder[A]): ConditionSQLBuilder[A] = {
    filter match {
      case IsNull(c)    => initial.isNull(g.column(c.field))
      case IsNotNull(c) => initial.isNotNull(g.column(c.field))
    }
  }

  private def trinary(
      filter: TrinaryFilter,
      initial: ConditionSQLBuilder[A]): ConditionSQLBuilder[A] = {
    filter match {
      case Between(column, lower, upper) =>
        initial.between(g.column(column.field), lower, upper)
      case NotBetween(column, lower, upper) =>
        initial.notBetween(g.column(column.field), lower, upper)
    }
  }

  def connectSeq(filters: Seq[Filter],
                 initial: ConditionSQLBuilder[A],
                 func: Transform[A],
                 bracket: Boolean): ConditionSQLBuilder[A] = {
    def connect(in: ConditionSQLBuilder[A]) = {
      val first =
        build(filters.head, bracket = true)(
          in.asInstanceOf[ConditionSQLBuilder[A]])
      filters.tail.foldLeft(first)((acc, filter) =>
        build(filter, bracket = true)(func(acc)))
    }
    if (bracket)
      initial.withRoundBracket[A] { in =>
        connect(in.asInstanceOf[ConditionSQLBuilder[A]])
      } else connect(initial)
  }

  private def build(filter: Filter, bracket: Boolean): Transform[A] = {
    initial =>
      filter match {
        case b: BinaryFilter  => binary(b, initial)
        case u: UnaryFilter   => unary(u, initial)
        case t: TrinaryFilter => trinary(t, initial)
        case a: And           => connectSeq(a.filters, initial, q => q.and, bracket)
        case o: Or            => connectSeq(o.filters, initial, q => q.or, bracket)
        case Not(f)           => build(f, bracket = true)(initial.not)
        case Like(column, pattern) =>
          initial.like(g.column(column.field), pattern)
      }
  }

  def build(filter: Filter): Transform[A] = build(filter, bracket = false)

}

object ConditionBuilder {
  def apply[A](implicit g: SyntaxProvider[A]): ConditionBuilder[A] =
    new ConditionBuilder

  implicit val binderValueType = new ParameterBinderFactory[ValueType] {
    def apply(value: ValueType): ParameterBinderWithValue =
      value match {
        case IntType(i)    => Binders.int(i)
        case StringType(s) => Binders.string(s)
        case DoubleType(d) => Binders.double(d)
      }

  }

}
