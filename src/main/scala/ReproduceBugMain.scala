import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by myhr on 24.03.17.
  */
object ReproduceBugMain extends TraitWithMethod {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    executeForEachpartitoin(sc.parallelize(1 to 100))
  }
}
