package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.scalike.ScalikeQueryBuilder.SyntaxProvider
import de.lmcoy.querybuilder._
import scalikejdbc._

object ConditionBuilder {

  type Transform[B] = ConditionSQLBuilder[B] => ConditionSQLBuilder[B]

  private def binary[A](filter: BinaryFilter, initial: ConditionSQLBuilder[A])
    : SyntaxProvider[A] => ConditionSQLBuilder[A] = syntaxProvider => {

    implicit val binderSQLType: ParameterBinderFactory[SQLType] = {
      case v: ValueType => binderValueType(v)
      case ColumnType(c) =>
        ParameterBinderFactory.sqlSyntaxParameterBinderFactory(
          syntaxProvider.column(c.field))
    }

    filter match {
      case Eq(l, r) => initial.eq(syntaxProvider.column(l.field), r)
      case Gt(l, r) => initial.gt(syntaxProvider.column(l.field), r)
      case Ne(l, r) => initial.ne(syntaxProvider.column(l.field), r)
      case Ge(l, r) => initial.ge(syntaxProvider.column(l.field), r)
      case Le(l, r) => initial.le(syntaxProvider.column(l.field), r)
      case Lt(l, r) => initial.lt(syntaxProvider.column(l.field), r)
    }
  }

  private def unary[A](filter: UnaryFilter, initial: ConditionSQLBuilder[A])
    : SyntaxProvider[A] => ConditionSQLBuilder[A] = syntaxProvider => {
    filter match {
      case IsNull(c)    => initial.isNull(syntaxProvider.column(c.field))
      case IsNotNull(c) => initial.isNotNull(syntaxProvider.column(c.field))
    }
  }

  private def trinary[A](filter: TrinaryFilter, initial: ConditionSQLBuilder[A])
    : SyntaxProvider[A] => ConditionSQLBuilder[A] = syntaxProvider => {
    filter match {
      case Between(column, lower, upper) =>
        initial.between(syntaxProvider.column(column.field), lower, upper)
      case NotBetween(column, lower, upper) =>
        initial.notBetween(syntaxProvider.column(column.field), lower, upper)
    }
  }

  def connectSeq[A](
      filters: Seq[Filter],
      initial: ConditionSQLBuilder[A],
      func: Transform[A],
      bracket: Boolean): SyntaxProvider[A] => ConditionSQLBuilder[A] =
    syntaxProvider => {
      def connect(in: ConditionSQLBuilder[A]) = {
        val first =
          build(filters.head, bracket = true)(syntaxProvider)(in)
        filters.tail.foldLeft(first)((acc, filter) =>
          build(filter, bracket = true)(syntaxProvider)(func(acc)))
      }
      if (bracket)
        initial.withRoundBracket { in =>
          connect(in.asInstanceOf[ConditionSQLBuilder[A]])
        } else connect(initial)
    }

  private def build[A](filter: Filter,
                       bracket: Boolean): SyntaxProvider[A] => Transform[A] =
    syntaxProvider => { initial =>
      filter match {
        case b: BinaryFilter  => binary(b, initial)(syntaxProvider)
        case u: UnaryFilter   => unary(u, initial)(syntaxProvider)
        case t: TrinaryFilter => trinary(t, initial)(syntaxProvider)
        case a: And =>
          connectSeq[A](a.filters, initial, q => q.and, bracket)(syntaxProvider)
        case o: Or =>
          connectSeq[A](o.filters, initial, q => q.or, bracket)(syntaxProvider)
        case Not(f) => build(f, bracket = true)(syntaxProvider)(initial.not)
        case Like(column, pattern) =>
          initial.like(syntaxProvider.column(column.field), pattern)
      }
    }

  def build[A](filter: Filter): SyntaxProvider[A] => Transform[A] =
    build(filter, bracket = false)

  implicit val binderValueType: ParameterBinderFactory[ValueType] = {
    case IntType(i)        => Binders.int(i)
    case StringType(s)     => Binders.string(s)
    case DoubleType(d)     => Binders.double(d)
    case BooleanType(b)    => Binders.boolean(b)
    case BigDecimalType(b) => Binders.bigDecimal(b)
    case BigIntType(b)     => Binders.bigInt(b)
    case DateType(b)       => Binders.sqlDate(b)
    case TimeType(b)       => Binders.sqlTime(b)
    case TimestampType(b)  => Binders.sqlTimestamp(b)
  }

}
