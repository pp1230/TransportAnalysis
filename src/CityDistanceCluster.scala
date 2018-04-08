import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{DoubleType, StructType}


/**
  * Created by pi on 18-4-7.
  */

object CityDistanceCluster {
  val cityDataPath = "/home/pi/Documents/DataSet/Transport/output/citydata2315"
  val filteredDataPath = "/home/pi/Documents/DataSet/Transport/output/FilteredData"
  val outputPath1 = "/home/pi/Documents/DataSet/Transport/output/GroupToGroupLines"
  val outputPath2 = "/home/pi/Documents/DataSet/Transport/output/GroupToGroupFilteredLines"
  val distance = 100000
  val weight = 300000

  def rad(d:Double): Double ={
    d * Math.PI / 180.00
  }
  def calculateDistance(longitude1:Double, latitude1:Double, longitude2:Double, latitude2:Double): Double ={
    val Lat1 = rad(latitude1); // 纬度
    val Lat2 = rad(latitude2);
    val a = Lat1 - Lat2;//两点纬度之差
    val b = rad(longitude1) - rad(longitude2); //经度之差
    var s = 2 * Math.asin(Math
        .sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(Lat1) * Math.cos(Lat2) * Math.pow(Math.sin(b / 2), 2)));//计算两点距离的公式
    s = s * 6378137.0;//弧长乘地球半径（半径为米）
    s = Math.round(s * 10000d) / 10000d;//精确距离的数值
    s
  }
  case class CityGroup(center: String, city: String)
  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val citylnglat = spark.read.csv(cityDataPath)
      .toDF("市", "站名", "lng", "lat")
    val allcity = citylnglat.select(
      citylnglat.col("市"), citylnglat.col("lng").cast(DoubleType), citylnglat.col("lat").cast(DoubleType)).distinct();
    allcity.show(false)
    println("CityNum:" + allcity.count())


    val dataArr = allcity.collect()
    var cityList = Seq(CityGroup("", ""))
    for (i <- 0 to dataArr.length - 1) {
      //var cityGroup: Map[String, String] = Map()
      //cityGroup += ("center" -> dataArr.apply(i).getString(0))
      for (j <- 0 to dataArr.length - 1) {
        //print(dataArr.apply(i).getDouble(1)+","+dataArr.apply(i).getDouble(2)+"/")
        //println(dataArr.apply(j).getDouble(1)+","+dataArr.apply(j).getDouble(2))

        if (calculateDistance(dataArr.apply(i).getDouble(1), dataArr.apply(i).getDouble(2),
          dataArr.apply(j).getDouble(1), dataArr.apply(j).getDouble(2)) < distance) {
          cityList :+= CityGroup(dataArr.apply(i).getString(0), dataArr.apply(j).getString(0))
          //cityList.toDF().show()
          //println(dataArr.apply(i).getString(0) + "-" + dataArr.apply(j).getString(0))
        }
      }
    }
    val cityFrame = cityList.toDF("中心", "城市")
    cityFrame.show()
    println("citycount:"+cityFrame.count())
    println("centercount:"+cityFrame.select("中心").distinct().count())
    val cityCount = cityFrame.groupBy("中心").count()
    val group = cityFrame.join(cityCount, "中心").filter($"count" > 1)
      .select("中心", "城市")
    group.show()


    val filtereddata = spark.read.csv(filteredDataPath)
      .toDF("品名","品名代码","发送城市", "发送城市经度", "发送城市纬度", "到达城市", "到达城市经度", "到达城市纬度",
        "物流总包（Y物流总包，N非物流总包）","车数","吨数")
    val sumcity = filtereddata.select(
      filtereddata.col("发送城市"), filtereddata.col("到达城市"),filtereddata.col("吨数").cast(DoubleType))
      .groupBy("发送城市", "到达城市").sum("吨数").filter($"sum(吨数)" < weight)
    sumcity.orderBy(desc("sum(吨数)")).show(false)
    println("SumLineCount:"+sumcity.count())
    val fgroupWeight = group.toDF("发送中心", "发送城市").join(sumcity, "发送城市")
    fgroupWeight.show(false)
    println("TotalCount1:"+fgroupWeight.count())
    val dgroupWeight = fgroupWeight.join(group.toDF("到达中心", "到达城市"), "到达城市")
    dgroupWeight.show(false)
    println("TotalCount2:"+dgroupWeight.count())
    val groupToGroup = dgroupWeight.select("发送中心", "到达中心", "sum(吨数)")
      .groupBy("发送中心", "到达中心").sum("sum(吨数)").orderBy(desc("sum(sum(吨数))"))
    groupToGroup.show(false)
    val filteredGroup = groupToGroup.filter($"sum(sum(吨数))">weight)
    println("GroupToGroupCount:"+groupToGroup.count())
    println("GroupToGroupFilteredCount:"+filteredGroup.count())
    groupToGroup.repartition(1).write.csv(outputPath1)
    filteredGroup.repartition(1).write.csv(outputPath2)

  }
}
