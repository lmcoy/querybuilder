package de.lmcoy.querybuilder

import scalikejdbc._

case class Group(id: Long, name: String)

object Group extends SQLSyntaxSupport[Group] {

  // If you need to specify schema name, override this
  // def table will return sqls"public.groups" in this case
  // Of course, schemaName doesn't work with MySQL
  override val schemaName = Some("public")

  // If the table name is same as snake_case'd name of this companion object,
  // you don't need to specify tableName explicitly.
  override val tableName = "members"

  // If you use NamedDB for this entity, override connectionPoolName
  //override val connectionPoolName = 'anotherdb

  def apply(g: ResultName[Group])(rs: WrappedResultSet) =
    new Group(rs.long(g.id), rs.string(g.name))
}

object QueryBuilder extends App {
  ConnectionPool.singleton("jdbc:h2:mem:hello", "user", "pass")

  // ad-hoc session provider on the REPL
  implicit val session = AutoSession

  // table creation, you can run DDL by using #execute as same as JDBC
  sql"""
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
""".execute.apply()

  // insert initial data
  Seq("Alice", "Bob", "Chris") foreach { name =>
    sql"insert into members (name, created_at) values (${name}, current_timestamp)".update
      .apply()
  }

  type SyntaxProvider[A] =
    QuerySQLSyntaxProvider[SQLSyntaxSupport[A], A]

  private def columnsToSQLSyntax[A](columns: List[Aggregation])(
      implicit g: SyntaxProvider[A]) = {
    columns
      .filter(_ match {
        case Id(_, _) => false
        case _        => true
      })
      .map(
        name => g.column(name.column.field)
      )
  }

  private def buildGroupBy[A](
      builder: ConditionSQLBuilder[A],
      columns: List[Aggregation])(implicit g: SyntaxProvider[A]) = {
    val cols = columnsToSQLSyntax(columns)
    cols match {
      case Nil => builder.groupBy()
      case _   => builder.groupBy(cols: _*)
    }
  }

  def buildQuery[A, B](g: SyntaxProvider[A],
                       columns: List[Aggregation],
                       extract: WrappedResultSet => B) = {
    implicit val ig = g

    val filter = And(Between("id", "0", StringType("1")), Eq("id", "1"))

    withSQL {
      val select = SelectBuilder(ig).build(columns).from(g.support as g)
      val where = ConditionBuilder(ig).build(filter)
      val w = where(select.where).asInstanceOf[ConditionSQLBuilder[A]]
      buildGroupBy(w, columns)
    }.map(extract(_))
  }

  val t = buildQuery(Group.syntax("g"),
                     List(Id(Column("id"), Some(Column("X"))),
                          Id(Column("name"), Some(Column("Y")))),
                     rs => rs.toMap())

  println("statement: " + t.statement)

  val x = t.list().apply
  x.foreach(println)

  def build[A](query: Query, tableSyntax: SyntaxProvider[A]) = {
    withSQL {
      // select
      val select = SelectBuilder(tableSyntax)
        .build(query.columns)
        .from(tableSyntax.support as tableSyntax)
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

}
