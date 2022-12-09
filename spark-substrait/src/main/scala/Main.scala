
import org.apache.spark.sql.SparkSession

object Main {
  def main(arg: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val csv = spark.read.option("header", true).csv("testdata/test.csv")
    csv.createTempView("test")

    val df = spark.sql("SELECT * FROM test WHERE id > 0")
    df.collect()

    val producer = new Producer()
    val rel = producer.toSubstraitRel(df.queryExecution.optimizedPlan)
    println(rel)
  }

}

