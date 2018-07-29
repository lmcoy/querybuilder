package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder._
import scalikejdbc.interpolation.SQLSyntax.{
  count,
  max,
  min,
  sum,
  avg,
  abs,
  ceil,
  floor
}
import scalikejdbc.{SQLSyntax, SelectSQLBuilder, select}
import de.lmcoy.querybuilder.scalike.ScalikeQueryBuilder.SyntaxProvider

class SelectBuilder[A](implicit g: SyntaxProvider[A]) {

  private[querybuilder] def agg(
      col: Column,
      alias: Option[Column],
      fun: Option[SQLSyntax => SQLSyntax] = None): scalikejdbc.SQLSyntax = {
    val c = fun match {
      case None    => g.column(col.field)
      case Some(f) => f(g.column(col.field))
    }
    alias match {
      case Some(a) => c + SQLSyntax.createUnsafely(s"as ${a.field}")
      case None    => c
    }
  }

  def build(columns: List[Aggregation]): SelectSQLBuilder[A] = {
    select(columns.map {
      case Id(col, alias)    => agg(col, alias)
      case Sum(col, alias)   => agg(col, alias, Some(sum))
      case Count(col, alias) => agg(col, alias, Some(count(_: SQLSyntax)))
      case Min(col, alias)   => agg(col, alias, Some(min))
      case Max(col, alias)   => agg(col, alias, Some(max))
      case Avg(col, alias)   => agg(col, alias, Some(avg))
      case Abs(col, alias)   => agg(col, alias, Some(abs))
      case Ceil(col, alias)  => agg(col, alias, Some(ceil))
      case Floor(col, alias) => agg(col, alias, Some(floor))
    }: _*)
  }

}

object SelectBuilder {
  def apply[A](implicit g: SyntaxProvider[A]): SelectBuilder[A] =
    new SelectBuilder
}
