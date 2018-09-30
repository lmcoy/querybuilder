package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.{Aggregation, Id}
import scalikejdbc._

object GroupByBuilder {
  private def columnsToSQLSyntax[A](
      columns: List[Aggregation]): SyntaxProvider[A] => Seq[SQLSyntax] =
    syntaxProvider => {
      columns
        .filter(_ match { // remove all aggregations
          case Id(_, _) => true
          case _        => false
        })
        .map(
          name => syntaxProvider.column(name.column.field)
        )
    }

  def build[A](
      builder: ConditionSQLBuilder[A],
      columns: List[Aggregation]): SyntaxProvider[A] => GroupBySQLBuilder[A] =
    syntaxProvider => {
      columnsToSQLSyntax(columns)(syntaxProvider) match {
        case Nil                => builder.groupBy()
        case c: List[SQLSyntax] => builder.groupBy(c: _*)
      }
    }
}
