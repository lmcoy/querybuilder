package de.lmcoy.querybuilder

sealed trait Filter

sealed trait BinaryFilter extends Filter {
  def left: Column
  def right: SQLType
}

/** > */
case class Gt(left: Column, right: SQLType) extends BinaryFilter

/** == */
case class Eq(left: Column, right: SQLType) extends BinaryFilter

/** != */
case class Ne(left: Column, right: SQLType) extends BinaryFilter

/** < */
case class Le(left: Column, right: SQLType) extends BinaryFilter

/** <= */
case class Lt(left: Column, right: SQLType) extends BinaryFilter

/** >= */
case class Ge(left: Column, right: SQLType) extends BinaryFilter

case class Like(column: Column, pattern: String) extends Filter

case class Or(filters: Filter*) extends Filter
case class And(filters: Filter*) extends Filter

sealed trait UnaryFilter extends Filter {
  def column: Column
}

case class IsNull(column: Column) extends UnaryFilter
case class IsNotNull(column: Column) extends UnaryFilter

case class Not(filter: Filter) extends Filter

sealed trait TrinaryFilter extends Filter

case class Between(column: Column, lower: ValueType, upper: ValueType)
    extends TrinaryFilter
case class NotBetween(column: Column, lower: ValueType, upper: ValueType)
    extends TrinaryFilter
