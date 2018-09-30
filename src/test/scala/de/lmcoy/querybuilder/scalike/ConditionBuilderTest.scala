package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.scalike.SelectBuilderTest.Table
import de.lmcoy.querybuilder._
import org.scalatest.Matchers
import org.scalatest.fixture.FlatSpec
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback

class ConditionBuilderTest extends FlatSpec with Matchers with AutoRollback {

  import ConditionBuilderTest._

  ConnectionPool.singleton("jdbc:h2:mem:hello", "user", "pass")

  override def fixture(implicit session: DBSession): Unit = {
    sql"""create table if not exists table (id serial not null primary key, name varchar(64))""".execute
      .apply()
  }

  "ConditionBuilder" should "be able to add a 'equals' where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, Eq("id", "1")).statement should equal(
        s"select $alias.id from table $alias  where  $alias.id = ?")

      query(columns, Eq("id", Column("id"))).statement should equal(
        s"select $alias.id from table $alias  where  $alias.id = $alias.id")
  }

  it should "be able to add a '<=' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Le("id", "1")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id <= ?")

    query(columns, Le("id", Column("id"))).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id <= $alias.id")
  }

  it should "be able to add a '<' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Lt("id", "1")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id < ?")

    query(columns, Lt("id", Column("id"))).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id < $alias.id")
  }

  it should "be able to add a '>=' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Ge("id", "1")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id >= ?")

    query(columns, Ge("id", Column("id"))).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id >= $alias.id")
  }

  it should "be able to add a '>' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Gt("id", "1")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id > ?")

    query(columns, Gt("id", Column("id"))).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id > $alias.id")
  }

  it should "be able to add a '<>' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Ne("id", "1")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id <> ?")

    query(columns, Ne("id", Column("id"))).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id <> $alias.id")
  }

  it should "be able to add a 'is null' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, IsNull("id")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id is null")
  }

  it should "be able to add a 'is not null' where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, IsNotNull("id")).statement should equal(
        s"select $alias.id from table $alias  where  $alias.id is not null")
  }

  it should "be able to add a 'between' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Between("id", 0, 20)).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id between ? and ?")
  }

  it should "be able to add a 'not between' where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, NotBetween("id", 0, 20)).statement should equal(
        s"select $alias.id from table $alias  where  not $alias.id between ? and ?")
  }

  it should "be able to add a 'not' to a filter in a where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, Not(Eq("id", 0.0))).statement should equal(
        s"select $alias.id from table $alias  where not  $alias.id = ?")
  }

  it should "be able to connect filters with 'and' in a where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, And(Eq("id", 1), Gt("id", 0), Le("id", 20))).statement should equal(
        s"select $alias.id from table $alias  where  $alias.id = ? and  $alias.id > ? and  $alias.id <= ?")
  }

  it should "be able to connect filters with 'or' in a where clause" in {
    implicit session =>
      val columns = List[Aggregation]("id")

      query(columns, Or(Eq("id", 1), Eq("id", 0), Eq("id", 20))).statement should equal(
        s"select $alias.id from table $alias  where  $alias.id = ? or  $alias.id = ? or  $alias.id = ?")
  }

  it should "be able to add a 'like' where clause" in { implicit session =>
    val columns = List[Aggregation]("id")

    query(columns, Like("id", "%test%")).statement should equal(
      s"select $alias.id from table $alias  where  $alias.id like ?")
  }

  it should "connect filters with 'and' and 'or'" in { implicit session =>
    val columns = List[Aggregation]("id")

    val filter = And(Or(Eq("id", 1), Eq("id", 5)),
                     Or(Eq("name", "Alice"), Eq("name", "Bob")))
    val expected =
      s"select $alias.id from table $alias  where (  $alias.id = ? or  $alias.id = ?) and (  $alias.name = ? or  $alias.name = ?)"
    query(columns, filter).statement should equal(expected)
  }

}

object ConditionBuilderTest {
  val alias = "t"
  implicit val t = Table.syntax(alias)

  def query(columns: List[Aggregation], filter: Filter) = {
    withSQL {
      val select = SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SelectSQLBuilder[Table]]
      ConditionBuilder
        .build(filter)(t)(select.where)
        .asInstanceOf[ConditionSQLBuilder[Table]]
    }
  }
}
