package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder._
import scalikejdbc.interpolation.SQLSyntax.{
  abs,
  avg,
  ceil,
  count,
  distinct => SQLDistinct,
  floor,
  max,
  min,
  sum
}
import scalikejdbc.{SQLSyntax, SelectSQLBuilder, select}
import de.lmcoy.querybuilder.scalike.ScalikeQueryBuilder.SyntaxProvider

object SelectBuilder {

  private[querybuilder] def agg[A](col: Column,
                                   alias: Option[Column],
                                   fun: Option[SQLSyntax => SQLSyntax] = None)
    : SyntaxProvider[A] => scalikejdbc.SQLSyntax =
    syntaxProvider => {
      val c = fun.fold(syntaxProvider.column(col.field))(f =>
        f(syntaxProvider.column(col.field)))
      alias.fold(c)(a => c + SQLSyntax.createUnsafely(s"as ${a.field}"))
    }

  def build[A](
      columns: List[Aggregation],
      distinct: Boolean = false): SyntaxProvider[A] => SelectSQLBuilder[A] =
    syntaxProvider => {
      val cols = columns
        .map { column =>
          def applyAggregation =
            (maybeAggFunc: Option[SQLSyntax => SQLSyntax]) =>
              agg[A](column.column, column.alias, maybeAggFunc)
          column match {
            case _: Id => applyAggregation(None)
            case Sum(_, isDistinct, _) if isDistinct =>
              applyAggregation(Some(c => sum(SQLDistinct(c))))
            case Sum(_, isDistinct, _) if !isDistinct =>
              applyAggregation(Some(sum))
            case Count(_, isDistinct, _) if isDistinct =>
              applyAggregation(Some(c => count(SQLDistinct(c))))
            case Count(_, isDistinct, _) if !isDistinct =>
              applyAggregation(Some(count))
            case _: Min   => applyAggregation(Some(min))
            case _: Max   => applyAggregation(Some(max))
            case _: Avg   => applyAggregation(Some(avg))
            case _: Abs   => applyAggregation(Some(abs))
            case _: Ceil  => applyAggregation(Some(ceil))
            case _: Floor => applyAggregation(Some(floor))
          }
        }
        .map(_(syntaxProvider))

      if (distinct)
        select(SQLDistinct(cols: _*))
      else
        select(cols: _*)
    }

}
