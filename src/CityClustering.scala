/**
  * Created by pi on 18-3-7.
  */

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.functions._

object CityClustering {
  val ClusterNumber = 110
  val Weight = 4
  val Distance = 10
  val DistanceFilter = 0.20
  val WeightFilter = 0.04
  //val outputPath = "/home/pi/Documents/DataSet/Transport/output/cityCN110W4D10DF0.2WF0.04"
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
    val alldata = spark.read.csv("/home/pi/Documents/DataSet/Transport/output/alldatafzsum2092")
        .select("_c0", "_c3", "_c4", "_c5", "_c1", "_c2")
      .toDF("市", "车数", "吨数", "收入", "lng", "lat")
    alldata.show(150)
    val nameFeature = alldata.map(row =>
      (row(0).toString, new DenseVector(Array(row(1).toString.toDouble, row(2).toString.toDouble,
        row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble))))
      .toDF("市", "features")
    nameFeature.show(false)

//    val dataFrame = spark.createDataFrame(Seq(
//      (0, Vectors.dense(1.0, 0.1, -1.0)),
//      (1, Vectors.dense(2.0, 1.1, 1.0)),
//      (2, Vectors.dense(3.0, 10.1, 3.0))
//    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")


    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(nameFeature)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(nameFeature)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("市", "features", "scaledFeatures").show(false)

    //使用最近的距离，聚集最为一致的吨数
    val weightedData = scaledData.map(row => {
      val array = row.getAs[DenseVector](2).toArray
      for(i <- 0 to array.size-1) {
        if(i == 1)
          array.update(i, array.apply(i)*Weight)
        if(i == 3||i == 4)
          array.update(i, array.apply(i)*Distance)
      }
      val weighted = new DenseVector(array)
      (row(0).toString, row.getAs[DenseVector](1), weighted)
    }).toDF("市", "features", "scaledFeatures")
    weightedData.show(false)

    // Trains a k-means model.
    val kmeans = new KMeans()
      .setK(ClusterNumber)
      .setSeed(1L)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("center")
    val model = kmeans.fit(weightedData)

    // Make predictions
    val predictions = model.transform(weightedData)
    predictions.show(false)

    println("Cluster Centers: ")
    val centers = model.clusterCenters

    val addCenter = predictions.map(row =>
      (row(0).toString, row.getAs[DenseVector](1), row.getAs[DenseVector](2),
        row(3).toString.toInt, centers.apply(row(3).toString.toInt)))
      .toDF("市", "features", "scaledFeatures", "cluster", "clusterFeature")
    addCenter.show(false)

    val result = addCenter.map(row => {
      val scaledFeatures = row.getAs[DenseVector](2)
      val clusterFeature = row.getAs[DenseVector](4)
      var sum = 0.0
      for(i <- 0 to scaledFeatures.size - 1){
        if(i==0||i==2)
          sum = sum + Math.pow((scaledFeatures.apply(i) - clusterFeature.apply(i)), 2)
        if(i==1)
          sum = sum + Math.pow((scaledFeatures.apply(i) - clusterFeature.apply(i)), 2)
        else
          sum = sum + Math.pow((scaledFeatures.apply(i) - clusterFeature.apply(i)), 2)
      }
      val distance = Math.sqrt(sum)
      (row(0).toString, row.getAs[DenseVector](1), row.getAs[DenseVector](2), row(3).toString.toInt,
        row.getAs[DenseVector](4), clusterFeature.apply(1), distance)
    })
    result.show(false)
    //按照吨数指标降序排列类别，相同类别按照距离升序排列。找出以较大吨数为中心的类别中，和中心最接近的几个城市，即每一类的前几个
    val cityCluster = result.select("_1","_4","_6","_7","_2","_5").toDF("市","类别","类别吨数指标","距离","原始数据","中心点")
        .filter($"距离">0&&$"距离"<DistanceFilter&&$"类别吨数指标">WeightFilter)
      .orderBy(desc("类别吨数指标"), asc("类别"), asc("距离"))

    cityCluster.show(1000,false)

//    val cityDistanceCluster = cityCluster.map(row => {
//      val rawData = row.getAs[DenseVector](4)
//      val lng = rawData.apply(3)
//      val lat = rawData.apply(4)
//      val m = calculateDistance()
//      (row(0).toString, row(1).toString.toInt,
//      row(2).toString.toDouble, row(3).toString.toDouble)})

    val mapResult = cityCluster.select("市", "类别", "原始数据").map(row => {
      val rawData = row.getAs[DenseVector](2)
      val weight = rawData.apply(1)
      val lng = rawData.apply(3)
      val lat = rawData.apply(4)
      (row(0).toString, row(1).toString.toInt, weight, lng, lat)
    }).toDF("市", "类别", "吨数", "经度", "纬度")

    mapResult.show(1000, false)

    //mapResult.repartition(1).write.csv(outputPath)

  }
}
