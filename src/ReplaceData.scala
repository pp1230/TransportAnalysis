/**
  * Created by pi on 18-4-2.
  */
import org.apache.spark.sql.functions._
object ReplaceData {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val alldata = spark.read.csv("/home/pi/Documents/DataSet/Transport/alldata0114.csv")
      .toDF("id","品名","品名代码","发局","发站","到局","到站"
        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
    alldata.show(false)
    println("AlldataCount:"+alldata.count())
    val citylnglat = spark.read.csv("/home/pi/Documents/DataSet/Transport/output/citydata2315")
      .toDF("市", "站名", "lng", "lat").distinct();
    citylnglat.show(false)
//    val citycount = citylnglat.select("市", "站名").groupBy("市", "站名").count()
//    citycount.orderBy(desc("count")).show(100)
//    println("AllcityCount:"+citylnglat.count())
//    println("AllcityCount:"+citylnglat.select("市", "站名").distinct().count())
    val startcity = alldata.join(citylnglat.toDF("发送城市", "发站", "发送城市经度", "发送城市纬度"), "发站")
    startcity.show(false)
    println("StartcityCount:"+startcity.count())
    val endcity = startcity.join(citylnglat.toDF("到达城市", "到站", "到达城市经度", "到达城市纬度"), "到站")
    endcity.show(false)
    println("EndcityCount:"+endcity.count())
    val filteredData = endcity.select("品名","品名代码","发送城市", "发送城市经度", "发送城市纬度", "到达城市", "到达城市经度", "到达城市纬度",
      "物流总包（Y物流总包，N非物流总包）","车数","吨数")
    filteredData.show(1000, false)
    filteredData.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/FilteredData")
  }

}
