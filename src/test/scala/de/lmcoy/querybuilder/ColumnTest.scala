package de.lmcoy.querybuilder

import org.scalatest._

class ColumnTest extends FlatSpec with Matchers {

  "Column" should "throw an exception if the name contains special characters or whitespaces" in {
    an[ColumnError] should be thrownBy Column("abc def")
    an[ColumnError] should be thrownBy Column("abc&a")
    an[ColumnError] should be thrownBy Column("a3,")
    an[ColumnError] should be thrownBy Column("from x; select * from y;")
  }

  it should "throw an exception if the name is empty" in {
    an[ColumnError] should be thrownBy Column("")
  }

  it should "consist of one word" in {
    Column("column1").field should equal("column1")
  }
}
