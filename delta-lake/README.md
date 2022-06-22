# Delta Lake Example

```bash
mvn package
export SPARK_HOME=/opt/spark-3.2.1-bin-hadoop3.2/
export SPARK_RAPIDS_PLUGIN_JAR=/home/andy/git/spark-rapids/dist/target/rapids-4-spark_2.12-22.08.0-SNAPSHOT-cuda11.jar
./run.sh
```


```scala
import org.apache.spark.sql._
import spark.implicits._

val df = Seq(1, 2, 3, 4).toDF("a")
df.write.mode(SaveMode.Overwrite).format("delta").save("/tmp/delta-table")

val df2 = spark.read.format("delta").load("/tmp/delta-table")
df2.show()
```