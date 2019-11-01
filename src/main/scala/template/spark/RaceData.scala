package template.spark

import org.apache.spark.sql.DataFrame

case class RaceData(circuits:DataFrame,
                    constructorResults:DataFrame,
                    constructorStandings:DataFrame,
                    constructors:DataFrame,
                    driverStandings:DataFrame,
                    drivers:DataFrame,
                    lapTimes:DataFrame,
                    pitStops:DataFrame,
                    qualifying:DataFrame,
                    races:DataFrame,
                    results:DataFrame,
                    seasons:DataFrame,
                    status:DataFrame){


}
