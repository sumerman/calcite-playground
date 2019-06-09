package calcite.playground

import java.sql.{Connection, DriverManager}
import java.util.Properties

import cats.effect.{IO, Resource}
import cats.implicits._
import org.scalatest._

class HelloCalciteSpec extends FlatSpec with Matchers with HelloCalcite {
  // AFAICT r.sales_city is equivalent to c.city
  // but utilizes an index on c.customer_region_id

  private val sqlQueries = List(
    """
      |select sum(unit_sales)
      |from sales_fact_1998 sf
      |join customer c on c.customer_id = sf.customer_id
      |join region r on r.region_id = c.customer_region_id
      |where r.sales_city = 'Albany'
    """.stripMargin,
    """
      |select t.month_of_year, t.the_month, sum(unit_sales)
      |from sales_fact_1998 sf
      |join customer c on c.customer_id = sf.customer_id
      |join region r on r.region_id = c.customer_region_id
      |join time_by_day t on t.time_id = sf.time_id
      |where r.sales_city = 'Albany'
      |group by 1,2
      |order by 1;
    """.stripMargin,
    """
      |select c.fullname
      |from sales_fact_1998 sf
      |join customer c on c.customer_id = sf.customer_id
      |join region r on r.region_id = c.customer_region_id
      |where r.sales_city = 'Albany'
      |group by sf.customer_id, c.fullname
      |order by sum(unit_sales) desc
      |limit 5;
    """.stripMargin
  )

  private def plainJdbcConnection[T](dbConf: SourceDBConfig) = {
    val props = new Properties()
    props.setProperty("user", dbConf.username)
    props.setProperty("password", dbConf.password)
    Class.forName(dbConf.jdbcDriver)

    Resource.fromAutoCloseable(IO(DriverManager.getConnection(dbConf.jdbcURL, props)))
  }

  private def statement(conn: Connection) = Resource.fromAutoCloseable(IO(conn.createStatement()))

  private def runSqlQueries() =
    plainJdbcConnection(DB_CONF).use { conn =>
      sqlQueries.traverse { query =>
        statement(conn).use { st =>
          val rows = collectRows(st.executeQuery(query))
          IO.pure(rows)
        }
      }
    }.unsafeRunSync()

  "RelBuilder queries" should "return the same ResultSet as SQL" in {
    val sqlResults = runSqlQueries()
    val relResults = runFoodmartQueries()

    (sqlResults zip relResults).foreach { case (sqlRes, relRes) =>
      sqlRes shouldEqual relRes
    }
  }
}
