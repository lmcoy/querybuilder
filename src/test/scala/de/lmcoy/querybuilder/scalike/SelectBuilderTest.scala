package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder._
import org.scalatest.Matchers
import org.scalatest.fixture.FlatSpec
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback

class SelectBuilderTest extends FlatSpec with Matchers with AutoRollback {

  import SelectBuilderTest.Table

  ConnectionPool.singleton("jdbc:h2:mem:hello", "user", "pass")
  val alias = "t"
  implicit val t = Table.syntax(alias)

  override def fixture(implicit session: DBSession): Unit = {
    sql"""create table if not exists table (id serial not null primary key, name varchar(64))""".execute
      .apply()

    def insert(name: String)(implicit session: DBSession = AutoSession) = {
      withSQL {
        val t = Table.column
        insertInto(Table).namedValues(t.name -> name)
      }.update().apply()
    }

    val entries = Seq("Alice", "Bob", "Chris")
    DB.localTx { _ =>
      entries.foreach(insert)
    }
  }

  "SelectBuilder" should "be able to load data from a database" in {
    implicit session =>
      val columns = List[Aggregation]("id" -> "X", "name" -> "Y")

      val sql = withSQL {
        SelectBuilder
          .build(columns)(t)
          .from(t.support as t)
          .asInstanceOf[SQLBuilder[Table]]
      }

      sql.statement should equal(
        s"select $alias.id as X, $alias.name as Y from table $alias")

      val result = sql.map(_.toMap()).toList().apply
      val expected = List(Map("X" -> 1, "Y" -> "Alice"),
                          Map("X" -> 2, "Y" -> "Bob"),
                          Map("X" -> 3, "Y" -> "Chris"))

      result should contain theSameElementsAs expected
  }

  it should "be able to use the aggregation 'sum'" in { implicit session =>
    val columns = List[Aggregation](Sum("id"), Sum("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select sum($alias.id), sum($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'count'" in { implicit session =>
    val columns = List[Aggregation](Count("id"), Count("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select count($alias.id), count($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'min'" in { implicit session =>
    val columns = List[Aggregation](Min("id"), Min("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select min($alias.id), min($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'max'" in { implicit session =>
    val columns = List[Aggregation](Max("id"), Max("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select max($alias.id), max($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'avg'" in { implicit session =>
    val columns = List[Aggregation](Avg("id"), Avg("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select avg($alias.id), avg($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'abs'" in { implicit session =>
    val columns = List[Aggregation](Abs("id"), Abs("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select abs($alias.id), abs($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'ceil'" in { implicit session =>
    val columns = List[Aggregation](Ceil("id"), Ceil("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select ceil($alias.id), ceil($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'floor'" in { implicit session =>
    val columns = List[Aggregation](Floor("id"), Floor("id" -> "X"))

    val sql = withSQL {
      SelectBuilder
        .build(columns)(t)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select floor($alias.id), floor($alias.id) as X from table $alias")
  }

  it should "be able to use 'select distinct' if the distinct flag is set" in {
    implicit session =>

    val sql = withSQL {
      SelectBuilder
        .build(List("id", "name"), distinct = true)(t)
        .from(t.support as t)
    }

    sql.statement should equal (
      s"select distinct $alias.id, $alias.name from table $alias"
    )
  }

  it should "be able to use 'count(distinct x)'" in { implicit session =>
    val sql = withSQL {
      SelectBuilder
        .build(List("id", Count("name", distinct = true, alias = Some("x"))))(t)
        .from(t.support as t)
    }

    sql.statement should equal (
      s"select $alias.id, count(distinct $alias.name) as x from table $alias"
    )
  }

  it should "be able to use 'sum(distinct x)'" in { implicit session =>
    val sql = withSQL {
      SelectBuilder
        .build(List("id", Sum("name", distinct = true, alias = Some("x"))))(t)
        .from(t.support as t)
    }

    sql.statement should equal (
      s"select $alias.id, sum(distinct $alias.name) as x from table $alias"
    )
  }
}

object SelectBuilderTest {

  case class Table(id: Int, name: String)

  object Table extends SQLSyntaxSupport[Table] {
    override val schemaName = None
    override val tableName = "table"
  }

}
