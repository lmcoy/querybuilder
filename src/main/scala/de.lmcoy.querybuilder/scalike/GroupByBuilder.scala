package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.{Aggregation, Id}
import scalikejdbc._

object GroupByBuilder {
  private def columnsToSQLSyntax[A](
      columns: List[Aggregation]): SyntaxProvider[A] => Seq[SQLSyntax] =
    syntaxProvider => {
      columns
        .filter(_ match { // remove all aggregations
          case _: Id => true
          case _        => false
        })
        .map(
          name => syntaxProvider.column(name.column.field)
        )
    }

  def build[A](
      builder: ConditionSQLBuilder[A],
      columns: List[Aggregation]): SyntaxProvider[A] => GroupBySQLBuilder[A] =
    syntaxProvider =>
      builder.groupBy(columnsToSQLSyntax(columns)(syntaxProvider): _*)
}
