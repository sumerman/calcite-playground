package calcite.playground

import com.typesafe.config.Config
import org.apache.calcite.adapter.jdbc.JdbcSchema

case class PgConfig(jdbcURL: String, username: String, password: String, calciteSchemaName: String) {
  def jdbcDataSource = JdbcSchema.dataSource(jdbcURL, "org.postgresql.Driver", username, password)
}
object PgConfig {
  def load(config: Config) = PgConfig(
    jdbcURL = config.getString("postgres.url"),
    username = config.getString("postgres.username"),
    password = config.getString("postgres.password"),
    calciteSchemaName = config.getString("postgres.calciteSchema"),
  )
}