import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.functions._

/**
  * Created by pi on 17-11-22.
  */
object Analysis {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    val goodsdata = spark.read.csv("/home/pi/Documents/DataSet/Transport/goods20162.csv")
      .toDF("id","票号","工作日期","品名","品名代码","发局","发站","到局","到站"
        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
    goodsdata.show()

    //---------------Sum---------------------
    val groupbyfazhansum = goodsdata.select(
      goodsdata.col("发站").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发站").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbyfazhansum.show(100,false)

    val groupbydaozhansum = goodsdata.select(
      goodsdata.col("到站").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("到站").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbydaozhansum.show(100,false)

    val groupbyfajusum = goodsdata.select(
      goodsdata.col("发局").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发局").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbyfajusum.show(100,false)

    val groupbydaojusum = goodsdata.select(
      goodsdata.col("到局").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("到局").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbydaojusum.show(100,false)

    val groupbyfahuorensum = goodsdata.select(
      goodsdata.col("发货人").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发货人").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbyfahuorensum.show(100,false)

    val groupbypinmingsum = goodsdata.select(
      goodsdata.col("品名").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("品名").sum("车数","吨数","收入").orderBy(desc("sum(收入)"))
    groupbypinmingsum.show(100,false)

    groupbyfazhansum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfazhansum")
    groupbydaozhansum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbydaozhansum")
    groupbyfajusum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfajusum")
    groupbydaojusum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbydaojusum")
    groupbyfahuorensum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfahuorensum")
    groupbypinmingsum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbypinmingsum")


    //---------------Avg---------------------

    val groupbyfazhanavg = goodsdata.select(
      goodsdata.col("发站").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发站").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbyfazhanavg.show(100,false)

    val groupbydaozhanavg = goodsdata.select(
      goodsdata.col("到站").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("到站").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbydaozhanavg.show(100,false)

    val groupbyfajuavg = goodsdata.select(
      goodsdata.col("发局").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发局").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbyfajuavg.show(100,false)

    val groupbydaojuavg = goodsdata.select(
      goodsdata.col("到局").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("到局").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbydaojuavg.show(100,false)

    val groupbyfahuorenavg = goodsdata.select(
      goodsdata.col("发货人").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("发货人").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbyfahuorenavg.show(100,false)

    val groupbypinmingavg = goodsdata.select(
      goodsdata.col("品名").cast(StringType), goodsdata.col("车数").cast(IntegerType),
      goodsdata.col("吨数").cast(DoubleType),goodsdata.col("收入").cast(DoubleType))
      .groupBy("品名").avg("车数","吨数","收入").orderBy(desc("avg(收入)"))
    groupbypinmingavg.show(100,false)

    groupbyfazhanavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfazhanavg")
    groupbydaozhanavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbydaozhanavg")
    groupbyfajuavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfajuavg")
    groupbydaojuavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbydaojuavg")
    groupbyfahuorenavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbyfahuorenavg")
    groupbypinmingavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/groupbypinmingavg")
  }
}
