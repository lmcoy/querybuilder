package de.lmcoy.querybuilder.scalike

import cats.data.Reader
import de.lmcoy.querybuilder.scalike.ScalikeQueryBuilder.QueryReader
import de.lmcoy.querybuilder.{DBQuery, Query, QueryBuilder}
import scalikejdbc._

class ScalikeQueryBuilder[A](implicit tableSyntax: SyntaxProvider[A],
                             session: DBSession)
    extends QueryBuilder {

  // lift to Reader
  private def buildSelect: QueryReader[SelectSQLBuilder[A]] =
    Reader(
      query =>
        SelectBuilder(tableSyntax)
          .build(query.columns)
          .from(tableSyntax.support as tableSyntax)
          .asInstanceOf[SelectSQLBuilder[A]]
    )

  // lift to Reader
  private def buildWhere(
      select: SelectSQLBuilder[A]): QueryReader[ConditionSQLBuilder[A]] =
    Reader(
      query =>
        query.filter
          .map(filter =>
            ConditionBuilder(tableSyntax).build(filter)(select.where))
          .getOrElse(select.where(None))
    )

  // lift to Reader
  private def buildGroupBy(
      filtered: ConditionSQLBuilder[A]): QueryReader[GroupBySQLBuilder[A]] =
    Reader(
      query => GroupByBuilder(tableSyntax).build(filtered, query.columns)
    )

  def buildSQL(query: Query) = {
    withSQL {
      val reader = for {
        select <- buildSelect
        filtered <- buildWhere(select)
        grouped <- buildGroupBy(filtered)
      } yield grouped
      reader.run(query)
    }
  }

  def build(query: Query) = {
    val q = buildSQL(query)

    new DBQuery {
      override def sql: String = q.statement
      override def query: () => List[Map[String, Any]] =
        q.map(_.toMap()).toList().apply _
    }
  }

}

object ScalikeQueryBuilder {
  type SyntaxProvider[A] =
    QuerySQLSyntaxProvider[SQLSyntaxSupport[A], A]

  type QueryReader[A] = Reader[Query, A]
}
