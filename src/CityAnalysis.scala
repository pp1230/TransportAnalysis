import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

/**
  * Created by pi on 17-11-22.
  */
object CityAnalysis {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    val goodsdata = spark.read.csv("/home/pi/Documents/DataSet/Transport/goods20162.csv")
      .toDF("id","票号","工作日期","品名","品名代码","发局","发站","到局","到站"
        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
    goodsdata.show()
    val transformfz = goodsdata.select(goodsdata.col("id"),
      regexp_replace(goodsdata.col("发站"), "['东','西','南','北','境']$","").name("站名")
      ,goodsdata.col("到站"))
    transformfz.show(100,false)
    val fz = goodsdata.join(transformfz, "id")
    fz.show(100,false)

    val transformdz = goodsdata.select(goodsdata.col("id"),
      goodsdata.col("发站")
      ,regexp_replace(goodsdata.col("到站"), "['东','西','南','北','境']$","").name("站名"))
    transformdz.show(100,false)
    val dz = goodsdata.join(transformdz, "id")
    dz.show(100,false)

    val citydata1 = spark.read.csv("/home/pi/Documents/DataSet/Transport/city.csv")
      .toDF().select("_c0","_c6","_c7","_c8").toDF("站名","市","lng","lat")
    val citydata2 = citydata1.select(regexp_replace(citydata1.col("站名"), "['站']$","").name("站名")
      , citydata1.col("市"), citydata1.col("lng"), citydata1.col("lat"))
    citydata2.show()

    val joinfz = fz.join(citydata2,"站名")
    joinfz.show()
    val joindz = dz.join(citydata2,"站名")
    joindz.show()

    //-------------------Analysis-----------------

    val citylnglat = citydata1.select(
      citydata1.col("市"),citydata1.col("lng").cast(DoubleType),citydata1.col("lat").cast(DoubleType))
      .groupBy("市").avg("lng","lat")

    val fzsum = joinfz.select(
      joinfz.col("市").cast(StringType), joinfz.col("车数").cast(IntegerType),
      joinfz.col("吨数").cast(DoubleType),joinfz.col("收入").cast(DoubleType))
      .groupBy("市").sum("车数","吨数","收入").join(citylnglat,"市")
      .orderBy(desc("sum(收入)"))
    fzsum.show()

    val dzsum = joindz.select(
      joindz.col("市").cast(StringType), joindz.col("车数").cast(IntegerType),
      joindz.col("吨数").cast(DoubleType),joindz.col("收入").cast(DoubleType))
      .groupBy("市").sum("车数","吨数","收入").join(citylnglat,"市")
      .orderBy(desc("sum(收入)"))
    dzsum.show()

    //fzsum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/cityfzsum")
    //dzsum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/citydzsum")

    val fzavg = joinfz.select(
      joinfz.col("市").cast(StringType), joinfz.col("车数").cast(IntegerType),
      joinfz.col("吨数").cast(DoubleType),joinfz.col("收入").cast(DoubleType))
      .groupBy("市").avg("车数","吨数","收入").join(citylnglat,"市")
      .orderBy(desc("avg(收入)"))
    fzavg.show()

    val dzavg = joindz.select(
      joindz.col("市").cast(StringType), joindz.col("车数").cast(IntegerType),
      joindz.col("吨数").cast(DoubleType),joindz.col("收入").cast(DoubleType))
      .groupBy("市").avg("车数","吨数","收入").join(citylnglat,"市")
      .orderBy(desc("avg(收入)"))
    dzavg.show()

    //fzavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/cityfzavg")
    //dzavg.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/citydzavg")
  }
}
