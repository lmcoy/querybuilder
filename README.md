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

# JSON input

The input JSON must have the following format:

```
{
    "columns": [...],
    "distinct": true|false,
    "filter": ... (optional),
    "limit": Int (optional)
}
```

The required content of the fields is specified in the following sections.

## columns

A column can be a simple string specifying a column that should be
selected from the table. To obtain a column under a different name,
the following JSON object can be used

```json
{
    "column": "nameInTable",
    "as": "newName"
}
```

It is also possible to use an aggregation function on a column.
The JSON object has to have the following format

```
{
    "func": "sum" | "avg" | "ceil" | "floor" | "abs" | "min" | "max" | "count",
    "column": "nameInTable",
    "as": "newName"
}
```

The functions `sum` and `count` can also contain the optional `distinct`
flag, i.e.

```
{
    "func": "sum" | "count",
    "column": "nameInTable",
    "as": "newName",
    "distinct": true | false
}
```

## filter

A filter is defined as tree of filter functions.


The basic filter type is a binary filter, e.g. the `=` comparison.
They can be specified with the following JSON object

```
{
    "type": "=" | ">" | "<" | ">=" | "<=" | "<>",
    "lhs": "columnName",
    "rhs": value | { "column": "columnName" }
}
```

The filters `between` and `not between` are also supported via

```
{
    "type": "between" | "not between",
    "lower": value,
    "upper": value
}
```

The filters `is null` and `is not null` are supported via

```
{
    "type": "is null" | "is not null",
    "column": "columnName"
}
```

The filter `like` is supported via

```json
{
    "type": "like",
    "column": "columnName",
    "pattern": "pattern"
}
```

More complex filters can be created by using `And` and `Or`.

```
{
    "type": "and" | "or",
    "filters": [list of filters]
}
```

For example the filter
```SQL
x = ? AND y IS NOT NULL
```

can be specified as

```json
{
    "type":"and",
    "filters": [
        {
            "type": "=",
            "lhs": "x",
            "rhs": 0
        },
        {
            "type": "is not null",
            "column": "y"
        }
    ]
}
```

Also `and` and `or` can be chained, i.e.

```SQL
(id = ? or  id = ?) and (name = ? or  name = ?)
```

is equivalent to

```json
{
    "type": "and",
    "filters": [
        {
            "type": "or",
            "filters": [
                { "type": "=", "lhs": "id", "rhs": 1 },
                { "type": "=", "lhs": "id", "rhs": 2 }
            ]
        }, {
            "type": "or",
            "filters": [
                { "type": "=", "lhs": "name", "rhs": "peter" },
                { "type": "=", "lhs": "name", "rhs": "steve" }
            ]
        },

    ]
}
```

The not operator is available via

```
{
    "type": "not",
    "filter": some filter
}
```