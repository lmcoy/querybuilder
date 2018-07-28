package de.lmcoy.querybuilder

import org.scalatest.Matchers
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback
import org.scalatest.fixture.FlatSpec

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
        SelectBuilder(t)
          .build(columns)
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
      SelectBuilder(t)
        .build(columns)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select sum($alias.id), sum($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'count'" in { implicit session =>
    val columns = List[Aggregation](Count("id"), Count("id" -> "X"))

    val sql = withSQL {
      SelectBuilder(t)
        .build(columns)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select count($alias.id), count($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'min'" in { implicit session =>
    val columns = List[Aggregation](Min("id"), Min("id" -> "X"))

    val sql = withSQL {
      SelectBuilder(t)
        .build(columns)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select min($alias.id), min($alias.id) as X from table $alias")
  }

  it should "be able to use the aggregation 'max'" in { implicit session =>
    val columns = List[Aggregation](Max("id"), Max("id" -> "X"))

    val sql = withSQL {
      SelectBuilder(t)
        .build(columns)
        .from(t.support as t)
        .asInstanceOf[SQLBuilder[Table]]
    }

    sql.statement should equal(
      s"select max($alias.id), max($alias.id) as X from table $alias")
  }

}

object SelectBuilderTest {

  case class Table(id: Int, name: String)

  object Table extends SQLSyntaxSupport[Table] {
    override val schemaName = None
    override val tableName = "table"
  }

}
