package calcite.playground

import com.typesafe.config.Config
import org.apache.calcite.adapter.jdbc.JdbcSchema

case class SourceDBConfig(jdbcURL: String, username: String, password: String, calciteSchemaName: String) {
  def jdbcDataSource = JdbcSchema.dataSource(jdbcURL, "org.postgresql.Driver", username, password)
  val jdbcDriver = "org.postgresql.Driver"
}

object SourceDBConfig {
  def load(config: Config) = SourceDBConfig(
    jdbcURL = config.getString("postgres.url"),
    username = config.getString("postgres.username"),
    password = config.getString("postgres.password"),
    calciteSchemaName = config.getString("postgres.calciteSchema"),
  )
}