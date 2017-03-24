import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by myhr on 24.03.17.
  */
/**
  * An input stream that always returns the same RDD on each time step. Useful for testing.
  */
class ConstantInputDStream[T: ClassTag](_ssc: StreamingContext, rdd: RDD[T])
  extends InputDStream[T](_ssc) {

  require(rdd != null,
    "parameter rdd null is illegal, which will lead to NPE in the following transformation")

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    Some(rdd)
  }
}
