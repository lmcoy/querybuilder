# querybuilder

`querybuilder` is a little scala library that uses [scalikejdbc]() to create
SQL queries that select rows from a database.
The query can be defined as case class or as json.

## Example: load query from json

For the examples we assume that the following table
was created in the database

```SQL
create table if not exists employees (
         id serial not null primary key,
         name varchar(64),
         dateofbirth date,
         city varchar(64),
         salary int
```

and we model the database with the following scala case class.

```scala
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

val alias = "t"
implicit val t = Employee.syntax(alias)
```

Now, we can use

```scala
val json =
  """
    |{
    |      "columns": ["city", {"func": "avg", "column": "salary"}],
    |      "distinct": false,
    |      "filter": { "type": ">", "lhs": "id", "rhs": 12 }
    |}
  """.stripMargin

val query = read[Query](json)

val b = new ScalikeQueryBuilder[Employee].build(query)
println(b.sql)
```

The generated SQL statement is

```SQL
select t.city, avg(t.salary) from employees t  where  t.id > ?  group by t.city
```

The query can be run with

```scala
val result: Seq[Map[String, Any]] = b.query()
```