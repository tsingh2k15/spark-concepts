import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DataFrameUDFTest extends FunSuite {

  val sparkSession =
    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

  test("should apply custom UDF on dataframe") {

    val dataset =
      sparkSession.
        createDataFrame(Seq((0, "Cricket"), (1, "Volleyball")))
        .toDF("id", "sport")

    val upper: String => String = _.toUpperCase

    import org.apache.spark.sql.functions.udf

    val upperUDF = udf(upper)

    val newDataSet = dataset.
      withColumn(
        "capSport",
        upperUDF(dataset("sport")
        )
      )

    newDataSet.show()

    assert(newDataSet.count() == 2)

  }
}
