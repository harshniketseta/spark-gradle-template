package template.spark

import org.apache.spark.sql.SparkSession

object TestFunctions {

  def addNaturalNumbers(n: Int)(implicit spark: SparkSession): Long = {
    spark.range(1, n+1).reduce(_ + _)
  }

}
