package io.andygrove.generatedot

import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, InputAdapter, SparkPlan}

import java.io.{BufferedWriter, FileWriter}

class SparkDot(plan: SparkPlan) {

  var nextCluster = 0

  def generate(): Unit = {
    val w = new BufferedWriter(new FileWriter("output.dot"))
    w.write("digraph G {\n")
    generate(w, plan)
    w.write("}\n")
    w.close()
  }

  def generate(w: BufferedWriter, plan: SparkPlan): Unit = {
    println(s"SparkDot.generate: ${plan.getClass}")

    // TODO subqueries in projections and filters

    plan match {
      case p: QueryStageExec =>
        val clusterId = nextCluster
        nextCluster += 1

        w.write(s"subgraph cluster$clusterId {\n")
        val label = s"${plan.nodeName}\nThis stage produced ${p.getRuntimeStatistics.rowCount} rows."
        w.write(s"""label = "$label";\n""")
        generate(w, p.plan)
        w.write(s"}\n")

      case p: FileSourceScanExec =>
        w.write(s"""node_${plan.id} [shape=box, label = "${p.simpleStringWithNodeId()}"];\n""")

      case _ =>
        w.write(s"""node_${plan.id} [shape=box, label = "${plan.nodeName}"];\n""")
        children(plan).foreach(ch => {
          generate(w, ch)
          w.write(s"node_${ch.id} -> node_${plan.id};\n")
        })
    }
  }

  def children(plan: SparkPlan): Seq[SparkPlan] = {
    plan match {
      case p: AdaptiveSparkPlanExec => Seq(p.executedPlan)
      case p: InputAdapter => Seq(p.child)
      case _ => plan.children
    }
  }

}

