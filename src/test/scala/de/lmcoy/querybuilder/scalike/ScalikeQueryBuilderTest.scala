package de.lmcoy.querybuilder.scalike

import de.lmcoy.querybuilder.{Avg, Lt, Query}
import org.scalatest.Matchers
import org.scalatest.fixture.FlatSpec
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback

class ScalikeQueryBuilderTest extends FlatSpec with Matchers with AutoRollback {

  import ScalikeQueryBuilderTest.Employee

  ConnectionPool.singleton("jdbc:h2:mem:hello", "user", "pass")
  val alias = "t"
  implicit val t = Employee.syntax(alias)

  override def fixture(implicit session: DBSession): Unit = {
    sql"""create table if not exists employees (
         id serial not null primary key,
         name varchar(64),
         dateofbirth date,
         city varchar(64),
         salary int
         )""".execute
      .apply()

    def insert(employee: Employee)(
        implicit session: DBSession = AutoSession) = {
      withSQL {
        val t = Employee.column
        insertInto(Employee).namedValues(
          t.name -> employee.name,
          t.dateOfBirth -> employee.dateOfBirth,
          t.city -> employee.city,
          t.salary -> employee.salary
        )
      }.update().apply()
    }

    val entries = Seq(
      Employee(0,
               "Alice",
               java.sql.Date.valueOf("1975-01-01"),
               "Berlin",
               10000),
      Employee(0, "Bob", java.sql.Date.valueOf("1973-11-11"), "Hamburg", 20000),
      Employee(0,
               "Chris",
               java.sql.Date.valueOf("1978-04-13"),
               "Hamburg",
               30000)
    )
    DB.localTx { _ =>
      entries.foreach(insert)
    }
  }

  "ScalikeQueryBuilder" should "be able to query a column" in {
    implicit session =>
      val q = Query(columns = List("name"), None)
      val data = new ScalikeQueryBuilder[Employee].queryList(q)

      data should contain(Map("NAME" -> "Alice"))
      data should contain(Map("NAME" -> "Bob"))
      data should contain(Map("NAME" -> "Chris"))
  }

  it should "be able to use aggregations" in { implicit session =>
    val q = Query(columns = List("city", Avg("Salary", Some("Salary"))), None)
    val data = new ScalikeQueryBuilder[Employee].queryList(q)

    data should contain(Map("CITY" -> "Hamburg", "SALARY" -> 25000))
    data should contain(Map("CITY" -> "Berlin", "SALARY" -> 10000))
  }

  it should "be able to use aggregations and filter" in { implicit session =>
    val q = Query(columns = List("city", Avg("Salary", Some("Salary"))),
                  Some(Lt("Salary", 25000)))
    val data = new ScalikeQueryBuilder[Employee].queryList(q)

    data should contain(Map("CITY" -> "Hamburg", "SALARY" -> 20000))
    data should contain(Map("CITY" -> "Berlin", "SALARY" -> 10000))
  }
}

object ScalikeQueryBuilderTest {
  case class Employee(
      id: Int,
      name: String,
      dateOfBirth: java.sql.Date,
      city: String,
      salary: Int
  )

  object Employee extends SQLSyntaxSupport[Employee] {
    override val schemaName = None
    override val tableName = "employees"
    override val useSnakeCaseColumnName = false
  }
}
