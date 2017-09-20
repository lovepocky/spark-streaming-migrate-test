package misc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.scalatest.FlatSpec

import scala.reflect.ClassTag

class MultiplexingTest extends FlatSpec with LazyLogging {

  def splitSample[T: ClassTag](rdd: RDD[T], n: Int, seed: Long = 42): Seq[RDD[T]] = {
    Vector.tabulate(n) { j =>
      rdd.mapPartitions { data =>
        scala.util.Random.setSeed(seed)
        data.filter { unused => scala.util.Random.nextInt(n) == j }
      }
    }
  }

  "spark rdd" should "split" in {
    FileStreamSink

  }
}
