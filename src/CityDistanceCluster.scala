/**
  * Created by pi on 18-4-7.
  */
object CityDistanceCluster {
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

  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val citylnglat = spark.read.csv("/home/pi/Documents/DataSet/Transport/output/citydata2315")
      .toDF("市", "站名", "lng", "lat").select("市", "lng", "lat").distinct();
    citylnglat.show(false)
    println("CityNum:"+citylnglat.count())

  }
}
