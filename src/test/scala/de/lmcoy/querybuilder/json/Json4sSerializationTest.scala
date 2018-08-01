package de.lmcoy.querybuilder.json

import de.lmcoy.querybuilder._
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.json4s.jackson.Serialization.{read, write}

import scala.reflect.ClassTag

class Json4sSerializationTest extends FlatSpec with Matchers {

  import Json4sSerialization.formats

  private def readAs[A: ClassTag, B](json: String)(
      implicit m: Manifest[B]): A = {
    val b = read[B](json)

    b shouldBe a[A]
    b.asInstanceOf[A]
  }

  "Json4sSerialization" should "be able to serialize a IntType to json" in {
    write(IntType(42)) should equal("42")
  }

  it should "be able to deserialize a BigIntType" in {
    readAs[BigIntType, SQLType]("42") should equal(BigIntType(42))
  }

  it should "be able to serialize and deserialize an integer" in {
    read[SQLType](write(BigIntType(42))) should equal(BigIntType(42))
  }

  // StringType

  it should "be able to serialize a StringType to json" in {
    write(StringType("hello world")) should equal("\"hello world\"")
  }

  it should "be able to deserialize a StringType" in {
    readAs[StringType, SQLType]("\"hello world\"") should equal(
      StringType("hello world"))
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
    readAs[DoubleType, SQLType]("42.0") should equal(DoubleType(42.0))
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
    readAs[BooleanType, SQLType]("false") should equal(BooleanType(false))
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
    readAs[DateType, SQLType]("{\"date\":\"1986-08-23\"}") should equal(
      DateType(java.sql.Date.valueOf("1986-08-23")))
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
    readAs[TimeType, SQLType]("{\"time\":\"23:24:13\"}") should equal(
      TimeType(java.sql.Time.valueOf("23:24:13")))
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
      "{\"timestamp\":\"2007-01-05 23:24:13.123\"}") should equal(
      TimestampType(java.sql.Timestamp.valueOf("2007-01-05 23:24:13.123")))
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
    readAs[ColumnType, SQLType]("{\"column\":\"id\"}") should equal(
      ColumnType("id"))
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
    val eq = readAs[Eq, Filter]("""{"type":"=","lhs":"x","rhs":42}""")

    eq.left should equal(Column("x"))
    eq.right should equal(BigIntType(42))
  }

  it should "be able to deserialize a Gt filter" in {
    val gt = readAs[Gt, Filter]("""{"type":">","lhs":"x","rhs":42}""")

    gt.left should equal(Column("x"))
    gt.right should equal(BigIntType(42))
  }

  it should "be able to deserialize a Ge filter" in {
    val ge = readAs[Ge, Filter]("""{"type":">=","lhs":"x","rhs":42}""")

    ge.left should equal(Column("x"))
    ge.right should equal(BigIntType(42))
  }

  it should "be able to deserialize a Le filter" in {
    val le = readAs[Le, Filter]("""{"type":"<=","lhs":"x","rhs":42}""")

    le.left should equal(Column("x"))
    le.right should equal(BigIntType(42))
  }

  it should "be able to deserialize a Lt filter" in {
    val lt = readAs[Lt, Filter]("""{"type":"<","lhs":"x","rhs":42}""")

    lt.left should equal(Column("x"))
    lt.right should equal(BigIntType(42))
  }

  it should "be able to deserialize a Ne filter" in {
    val ne = readAs[Ne, Filter]("""{"type":"<>","lhs":"x","rhs":42}""")

    ne.left should equal(Column("x"))
    ne.right should equal(BigIntType(42))
  }

  // IsNull

  it should "be able to serialize a IsNull filter" in {
    write[Filter](IsNull("x")) should equal(
      """{"type":"is null","column":"x"}""")
  }

  it should "be able to deserialize a IsNull filter" in {
    val isNull = readAs[IsNull, Filter]("""{"type":"is null","column":"x"}""")
    isNull should equal(IsNull("x"))
  }

  // IsNotNull

  it should "be able to serialize a IsNotNull filter" in {
    write[Filter](IsNotNull("x")) should equal(
      """{"type":"is not null","column":"x"}""")
  }

  it should "be able to deserialize a IsNotNull filter" in {
    val isNotNull =
      readAs[IsNotNull, Filter]("""{"type":"is not null","column":"x"}""")
    isNotNull should equal(IsNotNull("x"))
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
    val and = readAs[And, Filter](json)
    and.filters should equal(expected.filters.toList)
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
    val or = readAs[Or, Filter](json)
    or.filters should equal(expected.filters.toList)
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
    readAs[Between, Filter](json) should equal(expected)
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
    readAs[NotBetween, Filter](json) should equal(expected)
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
    readAs[Not, Filter](json) should equal(expected)
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
    readAs[Like, Filter](json) should equal(expected)
  }

}
