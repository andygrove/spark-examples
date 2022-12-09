import io.substrait.`type`.Type.Struct
import io.substrait.`type`.{ImmutableNamedStruct, Type}
import io.substrait.expression.Expression
import io.substrait.expression.Expression.BoolLiteral
import io.substrait.function.ImmutableSimpleExtension
import io.substrait.relation.files.ImmutableFileFormat.ParquetReadOptions
import io.substrait.relation.files.ImmutableFileOrFiles
import io.substrait.relation.{Filter, LocalFiles, Project, Rel}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.{logical => spark_logical}
import org.apache.spark.sql.catalyst.{expressions => spark_expr}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.collection.JavaConverters._

class Producer {
  def toSubstraitRel(plan: LogicalPlan): Rel = {
    plan match {
      case spark_logical.Project(exprs, child) =>
        Project.builder()
          .input(toSubstraitRel(child))
          .expressions(exprs.map(toSubstraitRex).asJava)
          .build()
      case spark_logical.Filter(cond, child) =>
        Filter.builder()
          .input(toSubstraitRel(child))
          .condition(toSubstraitRex(cond))
          .build()
      case LogicalRelation(rel, output, catalogTable, isStreaming) =>
        rel match {
          case HadoopFsRelation(location, partitionSchema, dataSchema, bucketSpec, fileFormat, options) =>
            //TODO do not assume Parquet
            val readOptions = ParquetReadOptions.builder()
              .build()
            // TODO add all files
            val files = ImmutableFileOrFiles.builder()
              .path(location.inputFiles.head)
              .fileFormat(readOptions)
              // TODO what do these options mean?
              .partitionIndex(0)
              .start(0)
              .length(1)
              .build()
            val fieldTypes: Seq[Type] = output.map(_.dataType).map {
              case t: DataType => t match {
                case DataTypes.IntegerType => Type.I32.builder().nullable(true).build()
                case DataTypes.StringType => Type.Str.builder().nullable(true).build()
                case _ =>
                  throw new UnsupportedOperationException(s"unsupported data type $t")
              }
            }
            val struct = Struct.builder()
              .addAllFields(fieldTypes.asJava)
              .nullable(false)
              .build()

            val schema = ImmutableNamedStruct.builder()
              .addAllNames(output.map(_.name).asJava)
              .struct(struct)
              .build()

            LocalFiles.builder()
              .initialSchema(schema)
              .addItems(files)
              .build()
          case _ =>
            throw new UnsupportedOperationException(s"unsupported LogicalRelation ${rel.getClass}")
        }
      case _ =>
        throw new UnsupportedOperationException(s"unsupported operator ${plan.getClass}")
    }
  }

  def toSubstraitRex(expr: spark_expr.Expression): Expression = {
    expr match {
      case spark_expr.Literal(value, dt) =>
        value match {
          case v: Boolean =>
            BoolLiteral.builder().value(v).build()
          case _ =>
            throw new UnsupportedOperationException(s"unsupported literal ${value.getClass}")
        }

      case spark_expr.AttributeReference(name, dataType, nullable, metadata) =>
        ???

      case spark_expr.IsNotNull(e) =>
        val ee = toSubstraitRex(e)

        ???

      case spark_expr.And(l, r) =>
        val ll = toSubstraitRex(l)
        val rr = toSubstraitRex(r)

        val function = ImmutableSimpleExtension.ScalarFunction.builder()
          .name("AND")
          .build()

        ???
      case _ =>
        throw new UnsupportedOperationException(s"unsupported expression ${expr.getClass}")
    }
  }
}
