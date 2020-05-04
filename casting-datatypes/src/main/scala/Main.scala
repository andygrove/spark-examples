
import java.util.TimeZone

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._

object Main {
  def main(arg: Array[String]) {

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    doCast(Seq[Long](0L, 6321706291L, Long.MaxValue).toDF("c0"))
    doCast(Seq[Float](0f, 6321706291f, Float.MaxValue).toDF("c0"))
    doCast(Seq[Double](0d, 6321706291d, Double.MaxValue).toDF("c0"))
  }

  def doCast(df: DataFrame): Unit = {
    df.collect().foreach(println)

    df.withColumn("c1", col("c0").cast(DataTypes.TimestampType))
      .withColumn("c2", col("c1").cast(DataTypes.DateType))
      .collect().foreach(println)
  }

}
