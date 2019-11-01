package template.spark

import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NationalityRaceDataSpec extends FlatSpec with Matchers with BeforeAndAfterAll with InitSpark {

  "Nationality Race Data" should
    "accept drivers and results data frame" in {
      val driversData = List(("1", "India"))
      import spark.implicits._
      val driversDf = driversData.toDF("driverId", "nationality")
      driversDf.show()

      val resultsData = List(("1", "1", "1"))
      val resultsDf = resultsData.toDF("driverId", "raceId", "position")
      resultsDf.show()

    val nationalityRaceData = NationalityRaceData(driversDf,resultsDf)
    assert(nationalityRaceData.isInstanceOf[NationalityRaceData])

  }

  "Nationality Race Data" should
    "count the number of races participated per nationality" in {

  }
}
