import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class PartitioningTest extends FunSuite {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()
    .sparkContext

  test("should load data from 2 partitions") {
    val input = spark.makeRDD(0 to 200000)

    input.filter(_ > 200).map(_ * 20).collect().toList

    assert(input.partitions.length == 2)
  }

  test("should load data from 10 partitions") {
    val input = spark.makeRDD(0 to 200000).repartition(10)

    input.filter(_ > 200).map(_ * 20).collect().toList

    assert(input.partitions.length == 10)
  }

}
