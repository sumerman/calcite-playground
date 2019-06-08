package calcite.playground

import java.sql.ResultSet

object HelloCalcite extends App with CalciteHelpers {
  private def printRows(res: ResultSet) = {
    val columnCount = res.getMetaData.getColumnCount
    while(res.next()) {
      val row = (1 to columnCount).map(res.getString)
      println(row)
    }
  }

  withFoodmart{ context =>
    val queryFactory = new FoodMartQueries(context.schema)

    val queries = Seq(
      queryFactory.sumInCity("Albany"),
      queryFactory.drillInCity("Albany"),
      queryFactory.topInCity("Albany", 5)
    )

    queries.foreach{ q =>
      context.executeQuery(q)(printRows)
      println("--")
    }
  }
}
