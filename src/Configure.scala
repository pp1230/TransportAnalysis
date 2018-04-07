import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 17-11-22.
  */
class Configure {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Transportation Analysis!")

  val ss = SparkSession.builder().config(conf).getOrCreate()
}
