package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.{Aggregation, Id}
import scalikejdbc._

class GroupByBuilder[A](implicit g: SyntaxProvider[A]) {
  private def columnsToSQLSyntax[A](columns: List[Aggregation])(
      implicit g: SyntaxProvider[A]): Seq[SQLSyntax] = {
    columns
      .filter(_ match { // remove all aggregations
        case Id(_, _) => true
        case _        => false
      })
      .map(
        name => g.column(name.column.field)
      )
  }

  def build(builder: ConditionSQLBuilder[A],
            columns: List[Aggregation]): GroupBySQLBuilder[A] = {
    columnsToSQLSyntax(columns) match {
      case Nil                => builder.groupBy()
      case c: List[SQLSyntax] => builder.groupBy(c: _*)
    }
  }
}

object GroupByBuilder {
  def apply[A](implicit g: SyntaxProvider[A]): GroupByBuilder[A] =
    new GroupByBuilder
}
