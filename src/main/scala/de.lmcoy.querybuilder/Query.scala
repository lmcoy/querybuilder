package de.lmcoy.querybuilder

case class Query(
    columns: List[Aggregation],
    distinct: Boolean,
    filter: Option[Filter] = None,
    limit: Option[Int] = None
)
