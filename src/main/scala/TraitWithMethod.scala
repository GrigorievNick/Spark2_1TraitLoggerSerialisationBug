import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by myhr on 24.03.17.
  */
trait TraitWithMethod {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def executeForEachpartitoin(parallelize: RDD[Int]): Unit = {
    parallelize
      .foreachPartition(it => {
//        val logger: Logger = LoggerFactory.getLogger(getClass)
        it.foreach{index =>
          println(s"${Thread.currentThread().getName} $index")
          logger.error(s"${Thread.currentThread().getName} $index")
        }
      })
  }
}
