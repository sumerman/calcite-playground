package calcite.playground

import java.sql.ResultSet
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

trait HelloCalcite extends CalciteHelpers {
  type StringRow = Vector[String]
  type StringResultSet = Seq[StringRow]

  val DB_CONF = SourceDBConfig.load(ConfigFactory.load())

  def collectRows(res: ResultSet): StringResultSet = {
    val columnCount = res.getMetaData.getColumnCount
    val acc = mutable.ArrayBuffer.empty[StringRow]

    while(res.next()) {
      val row = (1 to columnCount).map(res.getString).toVector
      acc += row
    }
    acc
  }

  def runFoodmartQueries(): Seq[StringResultSet] = {
    withCalciteConnection { conn =>
      val schema = addJdbcSchema(conn, DB_CONF)
      val queryFactory = new FoodMartQueries(schema)
      val city = "Albany"

      Seq(
        queryFactory.sumInCity(city),
        queryFactory.drillInCity(city),
        queryFactory.topInCity(city, 5)
      ).map{ q =>
        executeRelQuery(conn, q)(collectRows)
      }
    }
  }
}

object HelloCalcite extends App with HelloCalcite {
  runFoodmartQueries().foreach{ rows =>
    rows.foreach(println)
    println("----")
  }
}
