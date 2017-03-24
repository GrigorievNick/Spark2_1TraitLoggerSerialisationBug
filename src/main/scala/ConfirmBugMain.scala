import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by myhr on 24.03.17.
  */
object ConfirmBugMain {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    sc.parallelize(1 to 100)
      .foreachPartition(it => {
        //        val logger: Logger = LoggerFactory.getLogger(getClass)
        it.foreach { index =>
          logger.error(s"${Thread.currentThread().getName} $index")
          println(s"${Thread.currentThread().getName} $index")
        }
      })
  }
}

