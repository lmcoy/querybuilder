package de.lmcoy.querybuilder

case class ColumnError(msg: String) extends Exception {
  override def getMessage: String = msg
}

case class Column(field: String)

object Column {
  def apply(field: String): Column = {
    if ("""[a-zA-Z]\w*""".r.pattern.matcher(field).matches()) new Column(field)
    else throw ColumnError("not a valid column name")
  }

  implicit def stringToColumn(s: String): Column = Column(s)
}
