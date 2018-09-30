package de.lmcoy.querybuilder.scalike

import cats.data.Reader
import de.lmcoy.querybuilder.scalike.ScalikeQueryBuilder.{Environment, QueryReader}
import de.lmcoy.querybuilder.{DBQuery, Query, QueryBuilder}
import scalikejdbc.{SyntaxProvider, _}

class ScalikeQueryBuilder[A](implicit tableSyntax: SyntaxProvider[A],
                             session: DBSession)
    extends QueryBuilder {

  // lift to Reader
  private def buildSelect: QueryReader[A, SelectSQLBuilder[A]] =
    Reader(
      env =>
        SelectBuilder
          .build(env.query.columns)(env.syntaxProvider)
          .from(env.syntaxProvider.support as env.syntaxProvider)
          .asInstanceOf[SelectSQLBuilder[A]]
    )

  // lift to Reader
  private def buildWhere(
      select: SelectSQLBuilder[A]): QueryReader[A, ConditionSQLBuilder[A]] =
    Reader(
      env =>
        env.query.filter
          .map(filter =>
            ConditionBuilder.build(filter)(env.syntaxProvider)(select.where))
          .getOrElse(select.where(None))
    )

  // lift to Reader
  private def buildGroupBy(
      filtered: ConditionSQLBuilder[A]): QueryReader[A, GroupBySQLBuilder[A]] =
    Reader(
      env =>
        GroupByBuilder.build(filtered, env.query.columns)(env.syntaxProvider)
    )

  // lift to Reader
  private[scalike] def buildLimit(
      groupBy: GroupBySQLBuilder[A]): QueryReader[A, PagingSQLBuilder[A]] =
    Reader(env => env.query.limit.map(l => groupBy.limit(l)).getOrElse(groupBy))

  def buildSQL(query: Query): SQL[A, NoExtractor] = {
    withSQL {
      val reader = for {
        select <- buildSelect
        filtered <- buildWhere(select)
        grouped <- buildGroupBy(filtered)
        fin <- buildLimit(grouped)
      } yield fin
      reader.run(Environment(query, tableSyntax))
    }
  }

  def build(query: Query): DBQuery = {
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

  case class Environment[A](query: Query, syntaxProvider: SyntaxProvider[A])
  type QueryReader[A, B] = Reader[Environment[A], B]
}
