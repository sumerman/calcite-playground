package calcite.playground

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import cats.effect.{IO, Resource} // to ensure resources are closed properly

import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.SchemaPlus

trait CalciteHelpers {
  case class Context(schema: SchemaPlus, connection: CalciteConnection) {
    def executeQuery[T](query: RelNode)(action: ResultSet => T): T = executeRelQuery(connection, query)(action)
  }

  def addPgSchema(conn: CalciteConnection, conf: PgConfig): SchemaPlus = {
    Class.forName("org.postgresql.Driver")

    val schemaName = conf.calciteSchemaName
    val rootSchema = conn.getRootSchema
    val newSchema = JdbcSchema.create(rootSchema, schemaName, conf.jdbcDataSource, null, null)
    rootSchema.add(schemaName, newSchema)
    rootSchema.getSubSchema(schemaName)
  }

  def executeRelQuery[T](conn: CalciteConnection, query: RelNode)(action: ResultSet => T): T =
    prepareRelQuery(conn, query).flatMap { statement =>
      executePrepared(statement)
    }.use{ resultSet =>
      IO(action(resultSet))
    }.unsafeRunSync()

  def withCalciteConnection[T](action: CalciteConnection => T): T = {
    Class.forName("org.apache.calcite.jdbc.Driver")

    createCalciteConnection.use { calciteConn =>
      IO(action(calciteConn))
    }.unsafeRunSync()
  }

  def withFoodmart[T](action: Context => T): T =
    withCalciteConnection { conn =>
      import com.typesafe.config.ConfigFactory

      val pgConf = PgConfig.load(ConfigFactory.load())
      val schema = addPgSchema(conn, pgConf)
      action(Context(schema, conn))
    }

  private def prepareRelQuery(calciteConnection: CalciteConnection, query: RelNode) = {
    val prepareCtx = calciteConnection.createPrepareContext()
    val runner = prepareCtx.getRelRunner
    Resource.fromAutoCloseable(IO(runner.prepare(query)))
  }

  private def executePrepared(stmt: PreparedStatement) = Resource.fromAutoCloseable(IO(stmt.executeQuery()))

  private def createCalciteConnection: Resource[IO, CalciteConnection] = {
    val info = new Properties()
    info.setProperty("lex", "JAVA")

    Resource.fromAutoCloseable(IO {
      val conn = DriverManager.getConnection("jdbc:calcite:", info)
      conn.unwrap(classOf[CalciteConnection])
    })
  }
}
