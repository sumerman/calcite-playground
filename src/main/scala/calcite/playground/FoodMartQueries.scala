package calcite.playground

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}

trait FoodMartQueries {
  private def builderConfig(schema: SchemaPlus): FrameworkConfig =
    Frameworks
      .newConfigBuilder()
      .defaultSchema(schema)
      .build()

  def sumInCity(schema: SchemaPlus, cityName: String): RelNode = {
    val builder = RelBuilder.create(builderConfig(schema))

    builder
      .scan("customer")
      .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("city"), builder.literal(cityName)))
      .scan("sales_fact_1998")
      .join(JoinRelType.INNER, "customer_id")
      .aggregate(
        builder.groupKey(),
        builder.sum(builder.field("unit_sales"))
      )
      .build()
  }
}
