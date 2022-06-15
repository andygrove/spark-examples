
import org.apache.spark.sql.SparkSession
import io.delta.tables._

object Main {
  def main(arg: Array[String]) {

    val deltaFormat = "delta"

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(1, 2, 3, 4).toDF("a")
    df.write.format(deltaFormat).save("/tmp/delta-table")

    val df2 = spark.read.format(deltaFormat).load("/tmp/delta-table")
    df2.show()
  }

}
