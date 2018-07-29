package de.lmcoy.querybuilder

sealed trait SQLType

object SQLType {
  import ValueType._
  implicit def intToSQLType(i: Int): SQLType = i
  implicit def stringToSQLType(s: String): SQLType = s
  implicit def doubleToSQLType(d: Double): SQLType = d
  implicit def booleanToSQLType(v: Boolean): SQLType = v
  implicit def bigDecimalToSQLType(v: BigDecimal): SQLType = v
  implicit def bigIntToSQLType(v: BigInt): SQLType = v
  implicit def dateToSQLType(v: java.sql.Date): SQLType = v
  implicit def timeToSQLType(v: java.sql.Time): SQLType = v
  implicit def timestampToSQLType(v: java.sql.Timestamp): SQLType = v
  implicit def columnToSQLType(c: Column): SQLType = ColumnType(c)
}

sealed trait ValueType extends SQLType

object ValueType {
  implicit def intToValueType(i: Int): ValueType = IntType(i)
  implicit def stringToValueType(s: String): ValueType = StringType(s)
  implicit def doubleToValueType(d: Double): ValueType = DoubleType(d)
  implicit def booleanToValueType(v: Boolean): ValueType = BooleanType(v)
  implicit def bigDecimalToValueType(v: BigDecimal): ValueType = BigDecimalType(v)
  implicit def bigintToValueType(v: BigInt): ValueType = BigIntType(v)
  implicit def dateToValueType(v: java.sql.Date): ValueType = DateType(v)
  implicit def timeToValueType(v: java.sql.Time): ValueType = TimeType(v)
  implicit def timestampToValueType(v: java.sql.Timestamp): ValueType = TimestampType(v)
}

case class IntType(value: Int) extends ValueType
case class StringType(value: String) extends ValueType
case class DoubleType(value: Double) extends ValueType
case class BooleanType(value: Boolean) extends ValueType
case class BigDecimalType(value: BigDecimal) extends ValueType
case class BigIntType(value: BigInt) extends ValueType
case class DateType(value: java.sql.Date) extends ValueType
case class TimeType(value: java.sql.Time) extends ValueType
case class TimestampType(value: java.sql.Timestamp) extends ValueType

case class ColumnType(column: Column) extends SQLType
