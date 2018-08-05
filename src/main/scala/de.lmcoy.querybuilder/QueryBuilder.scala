package de.lmcoy.querybuilder

trait DBQuery {

  /** return the SQL query as string */
  def sql: String

  /** returns a function that actually sends the query to the DB */
  def query: () => List[Map[String, Any]]
}

trait QueryBuilder {
  def build(query: Query): DBQuery
}
