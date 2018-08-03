package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.{Query, QueryBuilder}
import scalikejdbc._

class ScalikeQueryBuilder[A](implicit tableSyntax: SyntaxProvider[A],
                             session: DBSession)
    extends QueryBuilder {

  def build(query: Query) = {
    withSQL {
      // select
      val select = SelectBuilder(tableSyntax)
        .build(query.columns)
        .from(tableSyntax.support as tableSyntax)
        .asInstanceOf[SelectSQLBuilder[A]]
      // add filter
      val filtered = query.filter
        .map(filter =>
          ConditionBuilder(tableSyntax).build(filter)(select.where))
        .getOrElse(select.where(None))
        .asInstanceOf[ConditionSQLBuilder[A]]
      // add group by
      GroupByBuilder(tableSyntax).build(filtered, query.columns)
    }
  }

  def queryList(query: Query): List[Map[String, Any]] = {
    build(query).map(_.toMap()).toList().apply
  }

}

object ScalikeQueryBuilder {
  type SyntaxProvider[A] =
    QuerySQLSyntaxProvider[SQLSyntaxSupport[A], A]
}
