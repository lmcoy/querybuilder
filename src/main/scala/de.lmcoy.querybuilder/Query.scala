package de.lmcoy.querybuilder

case class Query(
    columns: List[Aggregation],
    filter: Option[Filter] = None,
    limit: Option[Int] = None
)
