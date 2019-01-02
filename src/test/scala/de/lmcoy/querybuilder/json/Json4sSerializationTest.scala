package de.lmcoy.querybuilder.json

import de.lmcoy.querybuilder._
import org.json4s.MappingException
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.json4s.jackson.Serialization.{read, write}

class Json4sSerializationTest extends FlatSpec with Matchers {

  import Json4sSerialization.formats

  private def readAs[A, B](json: String)(f: A => Unit)(implicit mb: Manifest[B],
                                                       ma: Manifest[A]): Unit =
    read[B](json) match {
      case a: A => f(a)
      case t =>
        fail(
          s"unexpected type: ${t.getClass.getCanonicalName}, expected: ${ma.toString()}")
    }

  "Json4sSerialization" should "be able to serialize a IntType to json" in {
    write(IntType(42)) should equal("42")
  }

  it should "be able to deserialize a BigIntType" in {
    readAs[BigIntType, SQLType]("42") { int =>
      int should equal(BigIntType(42))
    }
  }

  it should "be able to serialize and deserialize an integer" in {
    read[SQLType](write(BigIntType(42))) should equal(BigIntType(42))
  }

  // StringType

  it should "be able to serialize a StringType to json" in {
    write(StringType("hello world")) should equal("\"hello world\"")
  }

  it should "be able to deserialize a StringType" in {
    readAs[StringType, SQLType]("\"hello world\"") {
      _ should equal(StringType("hello world"))
    }
  }

  it should "be able to serialize and deserialize an string" in {
    read[SQLType](write(StringType("hello world"))) should equal(
      StringType("hello world"))
  }

  // DoubleType

  it should "be able to serialize a DoubleType to json" in {
    write(DoubleType(42.0)) should equal("42.0")
  }

  it should "be able to deserialize a DoubleType" in {
    readAs[DoubleType, SQLType]("42.0") { _ should equal(DoubleType(42.0)) }
  }

  it should "be able to serialize and deserialize an DoubleType" in {
    val in = DoubleType(42.0)
    read[SQLType](write(in)) should equal(in)
  }

  // BooleanType

  it should "be able to serialize a BooleanType to json" in {
    write(BooleanType(true)) should equal("true")
  }

  it should "be able to deserialize a BooleanType" in {
    readAs[BooleanType, SQLType]("false") { _ should equal(BooleanType(false)) }
  }

  it should "be able to serialize and deserialize an BooleanType" in {
    val in = BooleanType(true)
    read[SQLType](write(in)) should equal(in)
  }

  // DateType

  it should "be able to serialize a DateType to json" in {
    write(DateType(java.sql.Date.valueOf("1986-08-23"))) should equal(
      "{\"date\":\"1986-08-23\"}")
  }

  it should "be able to deserialize a DateType" in {
    readAs[DateType, SQLType]("{\"date\":\"1986-08-23\"}") {
      _ should equal(DateType(java.sql.Date.valueOf("1986-08-23")))
    }
  }

  it should "be able to serialize and deserialize an DateType" in {
    val in = DateType(java.sql.Date.valueOf("1986-08-23"))
    read[SQLType](write(in)) should equal(in)
  }

  // TimeType

  it should "be able to serialize a TimeType to json" in {
    write(TimeType(java.sql.Time.valueOf("23:24:13"))) should equal(
      "{\"time\":\"23:24:13\"}")
  }

  it should "be able to deserialize a TimeType" in {
    readAs[TimeType, SQLType]("{\"time\":\"23:24:13\"}") {
      _ should equal(TimeType(java.sql.Time.valueOf("23:24:13")))
    }
  }

  it should "be able to serialize and deserialize an TimeType" in {
    val in = TimeType(java.sql.Time.valueOf("23:24:13"))
    read[SQLType](write(in)) should equal(in)
  }

  // TimestampType

  it should "be able to serialize a TimestampType to json" in {
    write(TimestampType(java.sql.Timestamp.valueOf("2007-01-05 23:24:13.123"))) should equal(
      "{\"timestamp\":\"2007-01-05 23:24:13.123\"}")
  }

  it should "be able to deserialize a TimestampType" in {
    readAs[TimestampType, SQLType](
      "{\"timestamp\":\"2007-01-05 23:24:13.123\"}") {
      _ should equal(
        TimestampType(java.sql.Timestamp.valueOf("2007-01-05 23:24:13.123")))
    }
  }

  it should "be able to serialize and deserialize an TimestampType" in {
    val in =
      TimestampType(java.sql.Timestamp.valueOf("2007-01-05 23:24:13.123"))
    read[SQLType](write(in)) should equal(in)
  }

  // ColumnType

  it should "be able to serialize a ColumnType to json" in {
    write(ColumnType("id")) should equal("{\"column\":\"id\"}")
  }

  it should "be able to deserialize a ColumnType" in {
    readAs[ColumnType, SQLType]("{\"column\":\"id\"}") {
      _ should equal(ColumnType("id"))
    }
  }

  it should "be able to serialize and deserialize an ColumnType" in {
    val in =
      ColumnType("id")
    read[SQLType](write(in)) should equal(in)
  }

  // BinaryFilter

  // write
  it should "be able to serialize a Eq filter" in {
    write(Eq("x", 42)) should equal("""{"type":"=","lhs":"x","rhs":42}""")
  }

  it should "be able to serialize a Gt filter" in {
    write(Gt("x", 42)) should equal("""{"type":">","lhs":"x","rhs":42}""")
  }

  it should "be able to serialize a Ne filter" in {
    write(Ne("x", 42)) should equal("""{"type":"<>","lhs":"x","rhs":42}""")
  }

  it should "be able to serialize a Lt filter" in {
    write(Lt("x", 42)) should equal("""{"type":"<","lhs":"x","rhs":42}""")
  }

  it should "be able to serialize a Le filter" in {
    write(Le("x", 42)) should equal("""{"type":"<=","lhs":"x","rhs":42}""")
  }

  it should "be able to serialize a Ge filter" in {
    write(Ge("x", 42)) should equal("""{"type":">=","lhs":"x","rhs":42}""")
  }

  // read
  it should "be able to deserialize a Eq filter" in {
    readAs[Eq, Filter]("""{"type":"=","lhs":"x","rhs":42}""") { eq =>
      eq.left should equal(Column("x"))
      eq.right should equal(BigIntType(42))
    }
  }

  it should "be able to deserialize a Gt filter" in {
    readAs[Gt, Filter]("""{"type":">","lhs":"x","rhs":42}""") { gt =>
      gt.left should equal(Column("x"))
      gt.right should equal(BigIntType(42))
    }
  }

  it should "be able to deserialize a Ge filter" in {
    readAs[Ge, Filter]("""{"type":">=","lhs":"x","rhs":42}""") { ge =>
      ge.left should equal(Column("x"))
      ge.right should equal(BigIntType(42))
    }
  }

  it should "be able to deserialize a Le filter" in {
    readAs[Le, Filter]("""{"type":"<=","lhs":"x","rhs":42}""") { le =>
      le.left should equal(Column("x"))
      le.right should equal(BigIntType(42))
    }
  }

  it should "be able to deserialize a Lt filter" in {
    readAs[Lt, Filter]("""{"type":"<","lhs":"x","rhs":42}""") { lt =>
      lt.left should equal(Column("x"))
      lt.right should equal(BigIntType(42))
    }
  }

  it should "be able to deserialize a Ne filter" in {
    readAs[Ne, Filter]("""{"type":"<>","lhs":"x","rhs":42}""") { ne =>
      ne.left should equal(Column("x"))
      ne.right should equal(BigIntType(42))
    }
  }

  // IsNull

  it should "be able to serialize a IsNull filter" in {
    write[Filter](IsNull("x")) should equal(
      """{"type":"is null","column":"x"}""")
  }

  it should "be able to deserialize a IsNull filter" in {
    readAs[IsNull, Filter]("""{"type":"is null","column":"x"}""") { isNull =>
      isNull should equal(IsNull("x"))
    }
  }

  // IsNotNull

  it should "be able to serialize a IsNotNull filter" in {
    write[Filter](IsNotNull("x")) should equal(
      """{"type":"is not null","column":"x"}""")
  }

  it should "be able to deserialize a IsNotNull filter" in {
    readAs[IsNotNull, Filter]("""{"type":"is not null","column":"x"}""") {
      isNotNull =>
        isNotNull should equal(IsNotNull("x"))
    }
  }

  // And

  it should "be able to serialize an And" in {
    val and = And(Eq("x", BigIntType(0)), IsNotNull("y"))
    val expected =
      """{"type":"and","filters":[{"type":"=","lhs":"x","rhs":0},{"type":"is not null","column":"y"}]}"""
    write[Filter](and) should equal(expected)
  }

  it should "be able to deserialize an And" in {
    val json =
      """{"type":"and","filters":[{"type":"=","lhs":"x","rhs":0},{"type":"is not null","column":"y"}]}"""
    val expected = And(Eq("x", BigIntType(0)), IsNotNull("y"))
    val and = readAs[And, Filter](json) { and =>
      and.filters should equal(expected.filters.toList)
    }
  }

  // Or

  it should "be able to serialize an Or" in {
    val or = Or(Eq("x", BigIntType(0)), IsNotNull("y"))
    val expected =
      """{"type":"or","filters":[{"type":"=","lhs":"x","rhs":0},{"type":"is not null","column":"y"}]}"""
    write[Filter](or) should equal(expected)
  }

  it should "be able to deserialize an Or" in {
    val json =
      """{"type":"or","filters":[{"type":"=","lhs":"x","rhs":0},{"type":"is not null","column":"y"}]}"""
    val expected = Or(Eq("x", BigIntType(0)), IsNotNull("y"))
    readAs[Or, Filter](json) { or =>
      or.filters should equal(expected.filters.toList)
    }
  }

  // Between

  it should "be able to serialize a Between" in {
    val between = Between("x", BigIntType(0), BigIntType(10))
    val expected = """{"type":"between","column":"x","lower":0,"upper":10}"""
    write[Filter](between) should equal(expected)
  }

  it should "be able to deserialize a Between" in {
    val json = """{"type":"between","column":"x","lower":0,"upper":10}"""
    val expected = Between("x", BigIntType(0), BigIntType(10))
    readAs[Between, Filter](json) { _ should equal(expected) }
  }

  // NotBetween

  it should "be able to serialize a NotBetween" in {
    val notBetween = NotBetween("x", BigIntType(0), BigIntType(10))
    val expected =
      """{"type":"not between","column":"x","lower":0,"upper":10}"""
    write[Filter](notBetween) should equal(expected)
  }

  it should "be able to deserialize a NotBetween" in {
    val json = """{"type":"not between","column":"x","lower":0,"upper":10}"""
    val expected = NotBetween("x", BigIntType(0), BigIntType(10))
    readAs[NotBetween, Filter](json) { _ should equal(expected) }
  }

  // Not

  it should "be able to serialize a Not" in {
    val not = Not(Eq("x", "y"))
    val expected =
      """{"type":"not","filter":{"type":"=","lhs":"x","rhs":"y"}}"""
    write[Filter](not) should equal(expected)
  }

  it should "be able to deserialize a Not" in {
    val expected = Not(Eq("x", "y"))
    val json = """{"type":"not","filter":{"type":"=","lhs":"x","rhs":"y"}}"""
    readAs[Not, Filter](json) { _ should equal(expected) }
  }

  // Like

  it should "be able to serialize a Like" in {
    val like = Like("x", "~test")
    val expected = """{"type":"like","column":"x","pattern":"~test"}"""
    write[Filter](like) should equal(expected)
  }

  it should "be able to deserialize a Like" in {
    val expected = Like("x", "~test")
    val json = """{"type":"like","column":"x","pattern":"~test"}"""
    readAs[Like, Filter](json) { _ should equal(expected) }
  }

  // Aggregation

  it should "be able to serialize an 'Id' aggregation" in {
    val id = Id(Column("id"))
    val expected = """"id""""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Id' aggregation" in {
    val expected = Id(Column("id"))
    val json =
      """
        |"id"
      """.stripMargin
    readAs[Id, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Id' aggregation with alias" in {
    val id = Id(Column("id"), Some("id2"))
    val expected = """{"column":"id","as":"id2"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Id' aggregation with alias" in {
    val expected = Id(Column("id"), Some("id2"))
    val json = """{"column":"id","as":"id2"}"""
    readAs[Id, Aggregation](json) { _ should equal(expected) }
  }

  // Sum

  it should "be able to serialize an 'Sum' aggregation" in {
    val id = Sum(Column("id"), alias = None)
    val expected = """{"func":"sum","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Sum' aggregation" in {
    val expected = Sum(Column("id"))
    val json = """{"func":"sum","column":"id"}"""
    readAs[Sum, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Sum' aggregation with alias" in {
    val id = Sum(Column("id"), alias = Some("alias"))
    val expected = """{"func":"sum","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Sum' aggregation with alias" in {
    val expected = Sum(Column("id"), alias = Some("alias"))
    val json = """{"func":"sum","column":"id","as":"alias"}"""
    readAs[Sum, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Sum' aggregation with distinct" in {
    val id = Sum(Column("id"), distinct = true)
    val expected = """{"func":"sum","column":"id","distinct":true}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Sum' aggregation with distinct" in {
    val expected = Sum(Column("id"), distinct = true)
    val json = """{"func":"sum","column":"id","distinct":true}"""
    readAs[Sum, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Sum' aggregation with distinct and alias" in {
    val id = Sum(Column("id"), distinct = true, alias = Some("alias"))
    val expected =
      """{"func":"sum","column":"id","as":"alias","distinct":true}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Sum' aggregation with distinct and alias" in {
    val expected = Sum(Column("id"), distinct = true, alias = Some("alias"))
    val json = """{"func":"sum","column":"id","as":"alias","distinct":true}"""
    readAs[Sum, Aggregation](json) { _ should equal(expected) }
  }

  // Count

  it should "be able to serialize an 'Count' aggregation" in {
    val id = Count(Column("id"), alias = None)
    val expected = """{"func":"count","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Count' aggregation" in {
    val expected = Count(Column("id"))
    val json = """{"func":"count","column":"id"}"""
    readAs[Count, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Count' aggregation with alias" in {
    val id = Count(Column("id"), alias = Some("alias"))
    val expected = """{"func":"count","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Count' aggregation with alias" in {
    val expected = Count(Column("id"), alias = Some("alias"))
    val json = """{"func":"count","column":"id","as":"alias"}"""
    readAs[Count, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Count' aggregation with distinct" in {
    val id = Count(Column("id"), distinct = true)
    val expected = """{"func":"count","column":"id","distinct":true}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Count' aggregation with distinct" in {
    val expected = Count(Column("id"), distinct = true)
    val json = """{"func":"count","column":"id","distinct":true}"""
    readAs[Count, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Count' aggregation with distinct and alias" in {
    val id = Count(Column("id"), distinct = true, alias = Some("alias"))
    val expected =
      """{"func":"count","column":"id","as":"alias","distinct":true}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Count' aggregation with distinct and alias" in {
    val expected = Count(Column("id"), distinct = true, alias = Some("alias"))
    val json = """{"func":"count","column":"id","as":"alias","distinct":true}"""
    readAs[Count, Aggregation](json) { _ should equal(expected) }
  }

  // Avg

  it should "be able to serialize an 'Avg' aggregation" in {
    val id = Avg(Column("id"), alias = None)
    val expected = """{"func":"avg","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Avg' aggregation" in {
    val expected = Avg(Column("id"))
    val json = """{"func":"avg","column":"id"}"""
    readAs[Avg, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Avg' aggregation with alias" in {
    val id = Avg(Column("id"), alias = Some("alias"))
    val expected = """{"func":"avg","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Avg' aggregation with alias" in {
    val expected = Avg(Column("id"), alias = Some("alias"))
    val json = """{"func":"avg","column":"id","as":"alias"}"""
    readAs[Avg, Aggregation](json) { _ should equal(expected) }
  }

  // Max

  it should "be able to serialize an 'Max' aggregation" in {
    val id = Max(Column("id"), alias = None)
    val expected = """{"func":"max","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Max' aggregation" in {
    val expected = Max(Column("id"))
    val json = """{"func":"max","column":"id"}"""
    readAs[Max, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Max' aggregation with alias" in {
    val id = Max(Column("id"), alias = Some("alias"))
    val expected = """{"func":"max","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Max' aggregation with alias" in {
    val expected = Max(Column("id"), alias = Some("alias"))
    val json = """{"func":"max","column":"id","as":"alias"}"""
    readAs[Max, Aggregation](json) { _ should equal(expected) }
  }

  // Min

  it should "be able to serialize an 'Min' aggregation" in {
    val id = Min(Column("id"), alias = None)
    val expected = """{"func":"min","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Min' aggregation" in {
    val expected = Min(Column("id"))
    val json = """{"func":"min","column":"id"}"""
    readAs[Min, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Min' aggregation with alias" in {
    val id = Min(Column("id"), alias = Some("alias"))
    val expected = """{"func":"min","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Min' aggregation with alias" in {
    val expected = Min(Column("id"), alias = Some("alias"))
    val json = """{"func":"min","column":"id","as":"alias"}"""
    readAs[Min, Aggregation](json) { _ should equal(expected) }
  }

  // Abs

  it should "be able to serialize an 'Abs' aggregation" in {
    val id = Abs(Column("id"), alias = None)
    val expected = """{"func":"abs","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Abs' aggregation" in {
    val expected = Abs(Column("id"))
    val json = """{"func":"abs","column":"id"}"""
    readAs[Abs, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Abs' aggregation with alias" in {
    val id = Abs(Column("id"), alias = Some("alias"))
    val expected = """{"func":"abs","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Abs' aggregation with alias" in {
    val expected = Abs(Column("id"), alias = Some("alias"))
    val json = """{"func":"abs","column":"id","as":"alias"}"""
    readAs[Abs, Aggregation](json) { _ should equal(expected) }
  }

  // Ceil

  it should "be able to serialize an 'Ceil' aggregation" in {
    val id = Ceil(Column("id"), alias = None)
    val expected = """{"func":"ceil","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Ceil' aggregation" in {
    val expected = Ceil(Column("id"))
    val json = """{"func":"ceil","column":"id"}"""
    readAs[Ceil, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Ceil' aggregation with alias" in {
    val id = Ceil(Column("id"), alias = Some("alias"))
    val expected = """{"func":"ceil","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Ceil' aggregation with alias" in {
    val expected = Ceil(Column("id"), alias = Some("alias"))
    val json = """{"func":"ceil","column":"id","as":"alias"}"""
    readAs[Ceil, Aggregation](json) { _ should equal(expected) }
  }

  // Floor

  it should "be able to serialize an 'Floor' aggregation" in {
    val id = Floor(Column("id"), alias = None)
    val expected = """{"func":"floor","column":"id"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Floor' aggregation" in {
    val expected = Floor(Column("id"))
    val json = """{"func":"floor","column":"id"}"""
    readAs[Floor, Aggregation](json) { _ should equal(expected) }
  }

  it should "be able to serialize an 'Floor' aggregation with alias" in {
    val id = Floor(Column("id"), alias = Some("alias"))
    val expected = """{"func":"floor","column":"id","as":"alias"}"""
    write[Aggregation](id) should equal(expected)
  }

  it should "be able to deserialize an 'Floor' aggregation with alias" in {
    val expected = Floor(Column("id"), alias = Some("alias"))
    val json = """{"func":"floor","column":"id","as":"alias"}"""
    readAs[Floor, Aggregation](json) { _ should equal(expected) }
  }

  // unknown aggregation function

  it should "throw an exception if the aggregation function is unknown" in {
    val json = """{"func":"f","column":"id"}"""

    the[MappingException] thrownBy read[Aggregation](json) should have message "unknown function 'f'"
  }
}
