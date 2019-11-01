package template.spark

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryRaceDataSpec extends FlatSpec with Matchers with BeforeAndAfterAll with InitSpark {

  "Query Race Data" should
    "return race data read" in {

    val raceData = QueryRaceData.loadData()
    assert(raceData.isInstanceOf[RaceData])

    raceData.circuits.columns shouldBe Array("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt", "url")
    raceData.circuits.count shouldBe 73
  }


  "Query Race Data" should
    "return the nationality data" in {

    val nationalityRaceData = QueryRaceData.loadNationalityData()
    assert(nationalityRaceData.isInstanceOf[NationalityRaceData])

    nationalityRaceData.drivers.columns shouldBe Array("driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality", "url")
    nationalityRaceData.drivers.count shouldBe 842

    nationalityRaceData.results.columns shouldBe Array("resultId", "raceId", "driverId", "constructorId", "number", "grid", "position", "positionText", "positionOrder", "points", "laps", "time", "milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed", "statusId")
    nationalityRaceData.results.count shouldBe 23777

    nationalityRaceData.countRaceByNationality().columns shouldBe Array("nationality", "racesParticipated")
    nationalityRaceData.countRaceByNationality().count shouldBe 41
  }


  //  override def afterAll(): Unit = {
  //    close
  //  }
}
