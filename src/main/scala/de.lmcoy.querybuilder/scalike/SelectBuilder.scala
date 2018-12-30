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
      columns: List[Aggregation], distinct: Boolean = false): SyntaxProvider[A] => SelectSQLBuilder[A] =
    syntaxProvider => {
      val cols = columns
          .map {
            case Id(col, alias)  => agg[A](col, alias)
            case Sum(col, isdist, alias)   =>
              if (isdist) agg[A](col, alias, Some(c => sum(SQLDistinct(c))))
              else agg[A](col, alias, Some(sum))
            case Count(col, isdist, alias) =>
              if (isdist) agg[A](col, alias, Some(c => count(SQLDistinct(c))))
              else agg[A](col, alias, Some(count))
            case Min(col, alias)   => agg[A](col, alias, Some(min))
            case Max(col, alias)   => agg[A](col, alias, Some(max))
            case Avg(col, alias)   => agg[A](col, alias, Some(avg))
            case Abs(col, alias)   => agg[A](col, alias, Some(abs))
            case Ceil(col, alias)  => agg[A](col, alias, Some(ceil))
            case Floor(col, alias) => agg[A](col, alias, Some(floor))
          }
          .map(_(syntaxProvider))

      if (distinct)
        select(SQLDistinct(cols: _*))
      else
        select(cols: _*)
    }

}
