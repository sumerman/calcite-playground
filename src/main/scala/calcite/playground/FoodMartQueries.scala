package calcite.playground

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}

class FoodMartQueries(schema: SchemaPlus) {
  private def builderConfig(schema: SchemaPlus): FrameworkConfig =
    Frameworks
      .newConfigBuilder()
      .defaultSchema(schema)
      .build()

  private val builder = RelBuilder.create(builderConfig(schema))

  private def customersFromCity(cityName: String) =
    builder
      .scan("customer")
      .scan("region")
      .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("sales_city"), builder.literal(cityName)))
      .join(JoinRelType.INNER, builder.call(
        SqlStdOperatorTable.EQUALS,
        builder.field(2, "customer", "customer_region_id"),
        builder.field(2, "region", "region_id")
      ))
      .build()

  def sumInCity(cityName: String): RelNode = {
    builder
      .push(customersFromCity(cityName))
      .scan("sales_fact_1998")
      .join(JoinRelType.INNER, "customer_id")
      .aggregate(
        builder.groupKey(),
        builder.sum(builder.field("unit_sales"))
      )
      .build()
  }

  def drillInCity(cityName: String): RelNode = {
    val withTimes = builder
        .scan("sales_fact_1998")
        .scan("time_by_day")
        .join(JoinRelType.INNER, "time_id")
        .build()

    builder
      .push(customersFromCity(cityName))
      .push(withTimes)
      .join(JoinRelType.INNER, "customer_id")
      .aggregate(
        builder.groupKey("month_of_year", "the_month"),
        builder.sum(false, "sales_sum", builder.field("unit_sales"))
      )
      .project(builder.field("month_of_year"), builder.field("the_month"), builder.field("sales_sum"))
      .sort(builder.field(0))
      .build()
  }

  def topInCity(cityName: String, limit: Int): RelNode = {
    builder
      .push(customersFromCity(cityName))
      .scan("sales_fact_1998")
      .join(JoinRelType.INNER, "customer_id")
      .aggregate(
        builder.groupKey("customer_id", "fullname"),
        builder.sum(false, "sales_sum", builder.field("unit_sales"))
      )
      .sortLimit(0, limit, builder.desc(builder.field("sales_sum")))
      .project(builder.field("fullname"))
      .build()
  }
}
