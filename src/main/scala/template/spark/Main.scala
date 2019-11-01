package template.spark

import org.apache.spark.sql.functions._

//final case class Person(firstName: String, lastName: String,
//                        country: String, age: Int)

object Main extends App with InitSpark {

  import spark.implicits._
  val nationalityRaceData = QueryRaceData.loadNationalityData()
  F1Writer.write(nationalityRaceData.countRaceByNationality(), "races_by_nationality")

  close

}
