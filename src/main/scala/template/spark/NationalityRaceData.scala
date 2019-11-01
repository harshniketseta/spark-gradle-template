package template.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct;
import org.apache.spark.sql.functions.count;
import org.apache.spark.sql.functions.desc;

case class NationalityRaceData(drivers: DataFrame,
                               results: DataFrame
                              ) {
  val driverResults = drivers.join(results, "driverId")

  def countRaceByNationality() = {
    driverResults.groupBy("nationality").agg(countDistinct("raceId").as("racesParticipated")).orderBy(desc("racesParticipated"))
  }

  def countRacesWonByNationality()={
    driverResults.filter("position==1").groupBy("nationality").agg(count("raceId").as("Races Won")).orderBy(desc("Races Won"))
  }
}
