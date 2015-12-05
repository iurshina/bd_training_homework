package org.yurshina.bytes

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, GivenWhenThen, Matchers}

class RequestCounterTest extends FlatSpec with GivenWhenThen with Matchers with BeforeAndAfter {

  private val master = "local[*]"
  private val appName = "test-spark"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  "Bytes" should "be counted" in {
    Given("lines")
    val lines = Array("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 " +
      "\"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",

      "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" " +
        "\"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"")

    When("count bytes")
    val res = IPCounter.count(sc, sc.parallelize(lines)).collect()

    Then("bytest counted")

    val elem = res(0)

    elem._1 should equal("ip1")
    elem._2 should equal(96956)
    elem._3 should equal(48478)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
