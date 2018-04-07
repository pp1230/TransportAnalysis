import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._

/**
  * Created by pi on 18-3-29.
  */
object WeightFilterClustring {

  val ClusterNumber = 130
  val Weight = 3
  val Distance = 50
  val outputPath = "/home/pi/Documents/DataSet/Transport/output/citydzClusters130w3d50"

  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val alldata = spark.read.csv("/home/pi/Documents/DataSet/Transport/output/alldatadzsum2315")
      .select("_c0", "_c3", "_c4", "_c5", "_c1", "_c2")
      .toDF("市", "车数", "weight", "收入", "lng", "lat")
    alldata.show(150)
//    alldata
//      .select("市", "weight", "lng", "lat")
//      .withColumn("cluster", lit(-1)).repartition(1).write.csv(outputPath)
    val bigweight = alldata.filter($"weight" >= 300000).select("市", "weight", "lng", "lat")
      .withColumn("cluster", lit(-1))
    val smallweight = alldata.filter($"weight" < 300000)
    val nameFeature = smallweight.map(row =>
      (row(0).toString, new DenseVector(Array(row(1).toString.toDouble, row(2).toString.toDouble,
        row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble)), row(2).toString.toDouble
        ,row(4).toString.toDouble, row(5).toString.toDouble))
      .toDF("市", "features", "weight", "lng", "lat")
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
      val array = row.getAs[DenseVector](5).toArray
      for(i <- 0 to array.size-1) {
        if(i == 1)
          array.update(i, array.apply(i)*Weight)
        if(i == 3||i == 4)
          array.update(i, array.apply(i)*Distance)
      }
      val weighted = new DenseVector(array)
      val weight = row.getDouble(2)
      val lng = row.getDouble(3)
      val lat = row.getDouble(4)
      (row(0).toString, row.getAs[DenseVector](1), weighted, weight, lng, lat)
    }).toDF("市", "features", "scaledFeatures", "weight", "lng", "lat")
    weightedData.show(false)

    // Trains a k-means model.
    val kmeans = new KMeans()
      .setK(ClusterNumber)
      .setSeed(1L)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("cluster")

    val model = kmeans.fit(weightedData)

    // Make predictions
    val predictions = model.transform(weightedData).orderBy("cluster")
    predictions.show(false)

    val clusters = predictions.select("weight", "cluster").groupBy("cluster").sum("weight")
    clusters.show(ClusterNumber)
    val filterdclusters = clusters.filter($"sum(weight)" > 300000).join(predictions, "cluster")
    filterdclusters.show(100,false)

    val result = filterdclusters.select("市", "cluster", "weight", "lng", "lat")
      .union(bigweight.select("市", "cluster", "weight", "lng", "lat"))
    result.show(1000, false)
    result.repartition(1).write.csv(outputPath)
  }
}
