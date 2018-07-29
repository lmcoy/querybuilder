package de.lmcoy.querybuilder

trait QueryBuilder {
  def queryList(query: Query): List[Map[String, Any]]
}
