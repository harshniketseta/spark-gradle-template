package template.spark

import template.spark.Main.spark

object F1Reader {
  val reader = spark.read
    .option("header",true)
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  def read(inputFile:String) = {
    reader.csv(inputFile)
  }
}
