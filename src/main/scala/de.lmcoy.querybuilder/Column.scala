package de.lmcoy.querybuilder
import scala.language.implicitConversions

case class ColumnError(msg: String) extends Exception {
  override def getMessage: String = msg
}

case class Column(field: String)

object Column {
  def apply(field: String): Column = {
    if ("""[a-zA-Z]\w*""".r.pattern.matcher(field).matches()) new Column(field)
    else throw ColumnError(s"not a valid column name: '$field'")
  }

  implicit def stringToColumn(s: String): Column = Column(s)
}
