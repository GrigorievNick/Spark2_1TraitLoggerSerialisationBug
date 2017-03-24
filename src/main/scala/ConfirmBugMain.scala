import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by myhr on 24.03.17.
  */
object ConfirmBugMain {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf().setMaster("local[4]").setAppName("test"), Duration(1000))

    new ConstantInputDStream(ssc, ssc.sparkContext.parallelize(1 to 100))
      .foreachRDD(rdd => {
        rdd
          .repartition(4)
          .foreachPartition(it => {
            val logger: Logger = LoggerFactory.getLogger(getClass)
            it.foreach { index =>
                logger.error(s"${Thread.currentThread().getName} $index")
                println(s"${Thread.currentThread().getName} $index")
            }
          })
      })

    ssc.start()
    ssc.awaitTerminationOrTimeout(2000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

