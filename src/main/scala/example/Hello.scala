package example

import org.apache.calcite.jdbc.CalciteConnection
import java.sql.DriverManager
import java.util.Properties

import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, Programs, RelBuilder}

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

case class PgConfig(jdbcURL: String, username: String, password: String) {
  def jdbcDataSource = JdbcSchema.dataSource(jdbcURL, "org.postgresql.Driver", username, password)
}
object PgConfig {
  def load(config: Config) = PgConfig(
    jdbcURL = config.getString("postgres.url"),
    username = config.getString("postgres.username"),
    password = config.getString("postgres.password")
  )
}

object Hello extends App {

  private val config = ConfigFactory.load()
  val pgConf = PgConfig.load(config)

  val info = new Properties()
  info.setProperty("lex", "JAVA")

  Class.forName("org.apache.calcite.jdbc.Driver")
  val connection = DriverManager.getConnection("jdbc:calcite:", info)
  val calciteConnection = connection.unwrap(classOf[CalciteConnection])
  val rootSchema = calciteConnection.getRootSchema

  Class.forName("org.postgresql.Driver")
  val schema = JdbcSchema.create(rootSchema, "foodmart", pgConf.jdbcDataSource, null, null)
  rootSchema.add("foodmart", schema)

  val prepareCtx = calciteConnection.createPrepareContext()
  val runner = prepareCtx.getRelRunner

  val relConfig = Frameworks
    .newConfigBuilder()
    //.parserConfig(SqlParser.Config.DEFAULT)
    .defaultSchema(rootSchema.getSubSchema("foodmart"))
    //.programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
    .build()

  val builder = RelBuilder.create(relConfig)

  val customers = builder
    .scan("customer")
    .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("city"), builder.literal("Albany")))
    .scan("sales_fact_1998")
    .join(JoinRelType.INNER, "customer_id")
    .aggregate(
      builder.groupKey(),
      builder.sum(builder.field("unit_sales"))
    )
    .build()

  val stmt = runner.prepare(customers)

  //val stmt = calciteConnection.createStatement()
  val res = stmt.executeQuery()//("select sum(unit_sales) from foodmart.sales_fact_1998 sf join foodmart.customer c on c.customer_id = sf.customer_id where c.city = 'Albany'")
  while(res.next()) {
    println(res.getString(1))
  }

  res.close()
  stmt.close()

  connection.close()

  println("")
}
