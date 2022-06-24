# Delta Lake Example

```bash
export SPARK_HOME=/opt/spark-3.2.1-bin-hadoop3.2/
export SPARK_RAPIDS_PLUGIN_JAR=/home/andy/git/spark-rapids/dist/target/rapids-4-spark_2.12-22.08.0-SNAPSHOT-cuda11.jar
```

```bash
$SPARK_HOME/bin/spark-shell \
  --master local[*] \
  --driver-memory 32g \
  --packages io.delta:delta-core_2.12:1.2.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars $SPARK_RAPIDS_PLUGIN_JAR \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin
```

```scala
import org.apache.spark.sql._
import spark.implicits._

val df = Seq(1, 2, 3, 4).toDF("a")
df.write.mode(SaveMode.Overwrite).format("delta").save("/tmp/delta-table")

val df2 = spark.read.format("delta").load("/tmp/delta-table")
df2.show()
```