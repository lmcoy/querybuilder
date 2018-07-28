package de.lmcoy.querybuilder

sealed trait SQLType

object SQLType {
  import ValueType._
  implicit def intToSQLType(i: Int): SQLType = i
  implicit def stringToSQLType(s: String): SQLType = s
  implicit def doubleToSQLType(d: Double): SQLType = d
  implicit def columnToSQLType(c: Column): SQLType = ColumnType(c)
}

sealed trait ValueType extends SQLType

object ValueType {
  implicit def intToValueType(i: Int): ValueType = IntType(i)
  implicit def stringToValueType(s: String): ValueType = StringType(s)
  implicit def doubleToValueType(d: Double): ValueType = DoubleType(d)
}

case class IntType(value: Int) extends ValueType
case class StringType(value: String) extends ValueType
case class DoubleType(value: Double) extends ValueType

case class ColumnType(column: Column) extends SQLType
