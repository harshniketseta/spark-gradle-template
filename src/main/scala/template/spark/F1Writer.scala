package template.spark

import org.apache.spark.sql.DataFrame

object F1Writer {
  val outputPath="/Users/harshniket.seta/workspace/thoughtworks/formula-1-race-results"
  def write(df:DataFrame, outputFile:String) = {
    df.write.parquet(s"${outputPath}/${outputFile}.parquet")
  }
}

