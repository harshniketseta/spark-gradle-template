package template.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct;
import org.apache.spark.sql.functions.desc;

case class NationalityRaceData(drivers:DataFrame,
                               results:DataFrame
                               ){
  def countRaceByNationality() = {
    val driverResults = drivers.join(results, "driverId")
    driverResults.groupBy("nationality").agg(countDistinct("raceId").as("racesParticipated")).orderBy(desc("racesParticipated"))
  }
}
