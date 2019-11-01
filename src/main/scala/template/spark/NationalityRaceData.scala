package template.spark

import org.apache.spark.sql.DataFrame

case class NationalityRaceData(drivers:DataFrame,
                               results:DataFrame
                               ){
  def countRaceByNationality() = {
    drivers.join(results, "driverId")
  }
}
