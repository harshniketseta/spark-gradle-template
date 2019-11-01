package template.spark

object QueryRaceData {
  val dataPath="/Users/harshniket.seta/workspace/thoughtworks/formula-1-race-data-19502017"

  def loadData(): RaceData = {
    RaceData(
      F1Reader.read(s"${dataPath}/circuits.csv"),
      F1Reader.read(s"${dataPath}/constructorResults.csv"),
      F1Reader.read(s"${dataPath}/constructorStandings.csv"),
      F1Reader.read(s"${dataPath}/constructors.csv"),
      F1Reader.read(s"${dataPath}/driverStandings.csv"),
      F1Reader.read(s"${dataPath}/drivers.csv"),
      F1Reader.read(s"${dataPath}/lapTimes.csv"),
      F1Reader.read(s"${dataPath}/pitStops.csv"),
      F1Reader.read(s"${dataPath}/qualifying.csv"),
      F1Reader.read(s"${dataPath}/races.csv"),
      F1Reader.read(s"${dataPath}/results.csv"),
      F1Reader.read(s"${dataPath}/seasons.csv"),
      F1Reader.read(s"${dataPath}/status.csv")
    )
  }

  def loadNationalityData(): NationalityRaceData = {
    NationalityRaceData(
      F1Reader.read(s"${dataPath}/drivers.csv"),
      F1Reader.read(s"${dataPath}/results.csv")
    )
  }
}