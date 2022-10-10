
import io.andygrove.generatedot.SparkDot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}

object Main {
  def main(arg: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(1,2,3,4).toDF("a")
    df.collect()

    new SparkDot(df.queryExecution.executedPlan).generate()
  }

}

