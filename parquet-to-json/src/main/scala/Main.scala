
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = trailArg[String]()
  val output = trailArg[String]()
  verify()
}

object Main {
  def main(arg: Array[String]) {
    val conf = new Conf(arg)

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.parquet(conf.input())
    df.write.json(conf.output())
  }

}
