package calcite.playground

import java.sql.ResultSet

object HelloCalcite extends App with CalciteHelpers with FoodMartQueries {
  private def printRows(res: ResultSet) = {
    val columnCount = res.getMetaData.getColumnCount
    while(res.next()) {
      val row = (1 to columnCount).map(res.getString)
      println(row)
    }
  }

  withFoodmart{ context =>
    val customers = sumInCity(context.schema, "Albany")

    context.executeQuery(customers)(printRows)
  }
}
