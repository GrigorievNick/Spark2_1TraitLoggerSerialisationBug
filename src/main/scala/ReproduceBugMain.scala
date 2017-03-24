import java.util.concurrent.Executors

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
  * Created by myhr on 24.03.17.
  */
object ReproduceBugMain extends TraitWithMethod {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    new ConstantInputDStream(ssc, sparkContext.parallelize(1 to 100))
      .foreachRDD(rdd => {
        implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))
        Await.ready(
          Future {
          executeForEachpartitoin(rdd)
        }, 100.seconds)
      })

    ssc.start()
    ssc.awaitTerminationOrTimeout(2000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
