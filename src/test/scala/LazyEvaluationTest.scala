import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class LazyEvaluationTest extends FunSuite {
  val spark: SparkContext =
    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
      .sparkContext

  test("result is not calculated when no action is applied") {
    val input = spark.makeRDD(List(1, 2, 3, 4))

    val rdd = input
      .filter(_ >= 3)
      .map(_ * 2)

    println(rdd.getNumPartitions)
    println(rdd.getStorageLevel)
    println(rdd.getCheckpointFile)
    println(rdd.toDebugString)
  }

  test("result is lazily calculated when an action is applied") {
    val input = spark.makeRDD(List(1, 2, 3, 4))

    val result = input
      .filter(_ >= 3)
      .map(_ * 2)
      .collect() // action
      .toList

    result should contain theSameElementsAs List(6, 8)
  }

}

