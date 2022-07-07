
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, LogicalQueryStage, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.rogach.scallop._

import scala.collection.mutable.ListBuffer

//class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//  val input = trailArg[String]()
//  val output = trailArg[String]()
//  verify()
//}

object Main {
  def main(arg: Array[String]) {
//    val conf = new Conf(arg)

    val enableCbo = true

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.plugins", "MySparkPlugin")
      .config(SQLConf.CBO_ENABLED.key, s"$enableCbo")
      .config(SQLConf.PLAN_STATS_ENABLED.key, s"$enableCbo")
      .config(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "MyExtensions")
      .getOrCreate()

    import spark.implicits._

//    val tables = Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
    val tables = Seq("customer", "orders", "lineitem")

    if (enableCbo) {
      tables.foreach { t =>
        spark.sql(s"CREATE TABLE $t USING parquet LOCATION '/mnt/bigdata/tpch/sf1-parquet/$t'").show()
        spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS").show()
        spark.sql(s"DESC EXTENDED $t").show()

        /*
        +--------------------+--------------------+-------+
        |            col_name|           data_type|comment|
        +--------------------+--------------------+-------+
        |          o_orderkey|              bigint|   null|
        |           o_custkey|              bigint|   null|
        |       o_orderstatus|              string|   null|
        |        o_totalprice|              double|   null|
        |         o_orderdate|                date|   null|
        |     o_orderpriority|              string|   null|
        |             o_clerk|              string|   null|
        |      o_shippriority|                 int|   null|
        |           o_comment|              string|   null|
        |                    |                    |       |
        |# Detailed Table ...|                    |       |
        |             Catalog|       spark_catalog|       |
        |            Database|             default|       |
        |               Table|              orders|       |
        |        Created Time|Thu Jul 07 16:40:...|       |
        |         Last Access|             UNKNOWN|       |
        |          Created By|Spark 3.4.0-SNAPSHOT|       |
        |                Type|            EXTERNAL|       |
        |            Provider|             parquet|       |
        |          Statistics|451389455 bytes, ...|       |
        +--------------------+--------------------+-------+
        */
      }
    } else {
      // register temp tables
      tables.foreach(t => spark.read.parquet(s"/mnt/bigdata/tpch/sf1-parquet/$t").createOrReplaceTempView(t))
    }

    val df = spark.sql("""select
                         |    l_orderkey,
                         |    sum(l_extendedprice * (1 - l_discount)) as revenue,
                         |    o_orderdate,
                         |    o_shippriority
                         |from
                         |    customer,
                         |    orders,
                         |    lineitem
                         |where
                         |        c_mktsegment = 'BUILDING'
                         |  and c_custkey = o_custkey
                         |  and l_orderkey = o_orderkey
                         |  and o_orderdate < date '1995-03-15'
                         |  and l_shipdate > date '1995-03-15'
                         |group by
                         |    l_orderkey,
                         |    o_orderdate,
                         |    o_shippriority
                         |order by
                         |    revenue desc,
                         |    o_orderdate""".stripMargin)

    df.show()
  }

}

class MySparkPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new MyDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new MyExecutorPlugin
}

class MyDriverPlugin extends DriverPlugin {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    super.init(sc, pluginContext)


  }
}

class MyExecutorPlugin extends ExecutorPlugin {
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectRuntimeOptimizerRule(sparkSession => new MyRule(sparkSession))
  }
}

class MyRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    println("[MyRule] plan=" + plan.getClass)

    val stages: Seq[LogicalQueryStage] = findLogicalQueryStages(plan, _.isInstanceOf[LogicalQueryStage])
      .map(_.asInstanceOf[LogicalQueryStage])

    for (stage <- stages) {
      val stats = stage.computeStats()
      println("computeStats: " + stats)
      /*
      computeStats: Statistics(sizeInBytes=4.6 MiB, rowCount=3.00E+5)
      computeStats: Statistics(sizeInBytes=114.8 MiB)
      computeStats: Statistics(sizeInBytes=308.8 MiB)
       */

      println(stats.attributeStats.map(c => c.toString()))
    }

    plan
  }

  def findLogicalQueryStages(plan: LogicalPlan, predicate: LogicalPlan => Boolean): Seq[LogicalPlan] = {
    def recurse(
                 plan: LogicalPlan,
                 predicate: LogicalPlan => Boolean,
                 accum: ListBuffer[LogicalPlan]): Seq[LogicalPlan] = {
      plan match {
        case _ if predicate(plan) =>
          accum += plan
          plan.children.flatMap(p => recurse(p, predicate, accum)).headOption
        // treat LogicalQueryStage as leaf node and do not recurse
        //case a: LogicalQueryStage => recurse(a.logicalPlan, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }
    recurse(plan, predicate, new ListBuffer[LogicalPlan]())
  }

}
