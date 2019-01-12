import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class OptimizeRDDTransformationsTest extends FunSuite {

  val sparkContext: SparkContext =
    SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
      .sparkContext

  test("filter after map") {
    val data = sparkContext.makeRDD(0 to 200000)

    val now = System.currentTimeMillis()
    val result = data.map(_ - 200).takeOrdered(100)
    println(s"filter after map took: ${System.currentTimeMillis() - now}")

    assert(result.length == 100)
  }

  test("map after filter") {
    val data = sparkContext.makeRDD(0 to 200000)

    val now = System.currentTimeMillis()
    val result = data.takeOrdered(100).map(_ - 200)
    println(s"map after filter took: ${System.currentTimeMillis() - now}")

    assert(result.length == 100)
  }

  //  filter after map took: 629

  //  map after filter took: 73

}
