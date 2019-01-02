package de.lmcoy.querybuilder.json

import de.lmcoy.querybuilder._
import org.json4s.{
  CustomSerializer,
  Extraction,
  Formats,
  MappingException,
  NoTypeHints
}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization

import scala.util.{Failure, Success, Try}

object Json4sSerialization {

  class ValueTypeSerializer
      extends CustomSerializer[ValueType](
        implicit format =>
          (
            {
              case JInt(i)     => BigIntType(i.intValue())
              case JDecimal(d) => BigDecimalType(d)
              case JDouble(d)  => DoubleType(d)
              case JBool(b)    => BooleanType(b)
              case JString(s)  => StringType(s)
              case j: JObject if j.values.contains("date") =>
                DateType(java.sql.Date.valueOf((j \ "date").extract[String]))
              case j: JObject if j.values.contains("time") =>
                TimeType(java.sql.Time.valueOf((j \ "time").extract[String]))
              case j: JObject if j.values.contains("timestamp") =>
                TimestampType(
                  java.sql.Timestamp.valueOf((j \ "timestamp").extract[String]))
            }, {
              case s: ValueType =>
                s match {
                  case IntType(i)        => JInt(i)
                  case DoubleType(d)     => JDouble(d)
                  case BigDecimalType(b) => JDecimal(b)
                  case BigIntType(i)     => JInt(i)
                  case BooleanType(b)    => JBool(b)
                  case StringType(str)   => JString(str)
                  case DateType(date)    => "date" -> date.toString
                  case TimeType(time)    => "time" -> time.toString
                  case TimestampType(ts) => "timestamp" -> ts.toString
                }
            }
        ))

  private val valueTypeSerializer = new ValueTypeSerializer

  class SQLTypeSerializer
      extends CustomSerializer[SQLType](
        implicit format =>
          (
            {
              case j: JObject if j.values.contains("column") =>
                Column((j \ "column").extract[String])
              case j: JValue => j.extract[ValueType]
            }, {
              case s: SQLType =>
                s match {
                  case v: ValueType =>
                    Extraction.decompose(v)(
                      Serialization
                        .formats(NoTypeHints) + valueTypeSerializer)
                  case c: ColumnType => "column" -> c.column.field
                }
            }
        ))

  private val opToBinaryFilter = Map(
    "=" -> Eq.apply _,
    "<>" -> Ne.apply _,
    ">" -> Gt.apply _,
    "<" -> Lt.apply _,
    ">=" -> Ge.apply _,
    "<=" -> Le.apply _
  )
  private val binaryFilterToOp =
    opToBinaryFilter.mapValues(_(Column("a"), IntType(0)).getClass).map(_.swap)

  class BinaryFilterSerializer
      extends CustomSerializer[BinaryFilter](implicit format =>
        (
          {
            case j: JObject =>
              val rhs = (j \ "rhs").extract[SQLType]
              val lhs = Column((j \ "lhs").extract[String])
              val op = (j \ "type").extract[String]
              opToBinaryFilter(op)(lhs, rhs)
          }, {
            case s: BinaryFilter =>
              ("type" -> binaryFilterToOp(s.getClass)) ~ ("lhs" -> s.left.field) ~ ("rhs" -> Extraction
                .decompose(s.right))
          }
      ))

  private def trinary(j: JObject,
                      f: (Column, ValueType, ValueType) => TrinaryFilter)(
      implicit formats: Formats): TrinaryFilter = {
    val column = Column((j \ "column").extract[String])
    val lower = (j \ "lower").extract[ValueType]
    val upper = (j \ "upper").extract[ValueType]
    f(column, lower, upper)
  }

  private def trinaryToJson(
      typ: String,
      column: Column,
      lower: ValueType,
      upper: ValueType)(implicit formats: Formats): JObject = {
    ("type" -> typ) ~ ("column" -> column.field) ~ ("lower" -> Extraction
      .decompose(lower)) ~
      ("upper" -> Extraction.decompose(upper))
  }

  class FilterSerializer
      extends CustomSerializer[Filter](implicit format =>
        (
          {
            case j: JObject =>
              val typ = (j \ "type").extract[String]
              typ match {
                case op if opToBinaryFilter.contains(op) =>
                  j.extract[BinaryFilter]
                case "like" =>
                  Like((j \ "column").extract[String],
                       (j \ "pattern").extract[String])
                case "is null"     => IsNull((j \ "column").extract[String])
                case "is not null" => IsNotNull((j \ "column").extract[String])
                case "not"         => Not((j \ "filter").extract[Filter])
                case "and"         => And((j \ "filters").extract[Seq[Filter]]: _*)
                case "or"          => Or((j \ "filters").extract[Seq[Filter]]: _*)
                case "between"     => trinary(j, Between.apply)
                case "not between" => trinary(j, NotBetween.apply)
              }

          }, {
            case f: Filter =>
              f match {
                case b: BinaryFilter =>
                  Extraction.decompose(b)(
                    Serialization
                      .formats(NoTypeHints) + new BinaryFilterSerializer)
                case Like(column, pattern) =>
                  ("type" -> "like") ~ ("column" -> column.field) ~ ("pattern" -> pattern)
                case IsNull(column) =>
                  ("type" -> "is null") ~ ("column" -> column.field)
                case IsNotNull(column) =>
                  ("type" -> "is not null") ~ ("column" -> column.field)
                case Not(filter) =>
                  ("type" -> "not") ~ ("filter" -> Extraction.decompose(filter))
                case and: And =>
                  ("type" -> "and") ~ ("filters" -> Extraction.decompose(
                    and.filters))
                case or: Or =>
                  ("type" -> "or") ~ ("filters" -> Extraction.decompose(
                    or.filters))
                case Between(column, lower, upper) =>
                  trinaryToJson("between", column, lower, upper)
                case NotBetween(column, lower, upper) =>
                  trinaryToJson("not between", column, lower, upper)
              }
          }
      ))

  class AggregationSerializer
      extends CustomSerializer[Aggregation](implicit format =>
        (
          {
            case jstring: JString =>
              val str = jstring.extract[String]
              Id(Column(str), None)
            case jobj: JObject =>
              import AggregationSerializer.jsonToDistinct

              val column = Column((jobj \ "column").extract[String])
              val alias =
                Try((jobj \ "as").extract[String]).toOption.map(Column.apply)
              Try((jobj \ "func").extract[String]) match {
                case Success(value) =>
                  value match {
                    case "sum" =>
                      Sum(column, distinct = jsonToDistinct(jobj), alias)
                    case "count" =>
                      Count(column, distinct = jsonToDistinct(jobj), alias)
                    case "max" =>
                      Max(column, alias)
                    case "min" =>
                      Min(column, alias)
                    case "avg" =>
                      Avg(column, alias)
                    case "abs" =>
                      Abs(column, alias)
                    case "ceil" =>
                      Ceil(column, alias)
                    case "floor" =>
                      Floor(column, alias)
                    case f =>
                      throw new MappingException(s"unknown function '$f'")
                  }
                case Failure(_) => Id(column, alias)
              }

          }, {
            case Id(column, alias) =>
              alias match {
                case None => JString(column.field)
                case as =>
                  AggregationSerializer.aggToJson(None, column, as)
              }
            case Sum(column, distinct, as) =>
              AggregationSerializer
                .aggToJson(Some("sum"), column, as) ~ AggregationSerializer
                .distinctToJson(distinct)
            case Count(column, distinct, as) =>
              AggregationSerializer
                .aggToJson(Some("count"), column, as) ~ AggregationSerializer
                .distinctToJson(distinct)
            case Avg(column, as) =>
              AggregationSerializer.aggToJson(Some("avg"), column, as)
            case Min(column, as) =>
              AggregationSerializer.aggToJson(Some("min"), column, as)
            case Max(column, as) =>
              AggregationSerializer.aggToJson(Some("max"), column, as)
            case Ceil(column, as) =>
              AggregationSerializer.aggToJson(Some("ceil"), column, as)
            case Floor(column, as) =>
              AggregationSerializer.aggToJson(Some("floor"), column, as)
            case Abs(column, as) =>
              AggregationSerializer.aggToJson(Some("abs"), column, as)
            case a: Aggregation =>
              throw new MappingException(
                s"${a.getClass.getCanonicalName} not serializable (not implemented)")
          }
      ))

  object AggregationSerializer {
    private def aggToJson(func: Option[String],
                          column: Column,
                          as: Option[Column]) = {
      val obj = ("column" -> column.field) ~ ("as" -> as.map(_.field))
      func match {
        case None    => obj
        case Some(f) => ("func" -> f) ~ obj
      }

    }

    private def distinctToJson(distinct: Boolean) = {
      "distinct" -> (if (distinct) Some(true) else None)
    }

    private def jsonToDistinct(jobj: JObject)(implicit formats: Formats) = {
      Try((jobj \ "distinct").extract[Boolean]).getOrElse(false)
    }

  }

  implicit val formats = Serialization.formats(NoTypeHints) +
    new ValueTypeSerializer +
    new SQLTypeSerializer +
    new FilterSerializer +
    new BinaryFilterSerializer +
    new AggregationSerializer

}
