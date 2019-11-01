package template.spark

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class F1ReaderSpec extends FlatSpec with Matchers with BeforeAndAfterAll with InitSpark {

  "File Reader" should
    "return data read from file" in {

    val output  = F1Reader.read("people-example.csv")
    output.columns shouldBe Array("firstName", "lastName", "country", "age")
    output.count shouldBe 7
  }

//  override def afterAll(): Unit = {
//    close
//  }
}
