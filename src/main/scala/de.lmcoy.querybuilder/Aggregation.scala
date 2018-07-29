package de.lmcoy.querybuilder

trait Aggregation {
  def column: Column
  def alias: Option[Column]
}

object Aggregation {
  implicit def stringWithAliasToId(s: (String, String)): Id =
    Id(Column(s._1), Some(Column(s._2)))

  implicit def stringToId(s: String): Id = Id(Column(s))
}

case class Id(column: Column, alias: Option[Column] = None) extends Aggregation

case class Sum(column: Column, alias: Option[Column] = None) extends Aggregation

object Sum {
  def apply(s: (String, String)): Sum =
    new Sum(Column(s._1), Some(Column(s._2)))
}

case class Count(column: Column, alias: Option[Column] = None)
    extends Aggregation

object Count {
  def apply(s: (String, String)): Count =
    new Count(Column(s._1), Some(Column(s._2)))
}

case class Max(column: Column, alias: Option[Column] = None) extends Aggregation

object Max {
  def apply(s: (String, String)): Max =
    new Max(Column(s._1), Some(Column(s._2)))
}

case class Min(column: Column, alias: Option[Column] = None) extends Aggregation

object Min {
  def apply(s: (String, String)): Min =
    new Min(Column(s._1), Some(Column(s._2)))
}