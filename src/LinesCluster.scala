import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by pi on 18-4-4.
  */
object LinesCluster {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val filtereddata = spark.read.csv("/Users/chenyang/Downloads/FilteredData.csv")
      .toDF("品名","品名代码","发送城市", "发送城市经度", "发送城市纬度", "到达城市", "到达城市经度", "到达城市纬度",
        "物流总包（Y物流总包，N非物流总包）","车数","吨数")
    val sumcity = filtereddata.select(
      filtereddata.col("发送城市"), filtereddata.col("到达城市"),filtereddata.col("吨数").cast(DoubleType))
      .groupBy("发送城市", "到达城市").sum("吨数")
    sumcity.orderBy(desc("sum(吨数)")).show(1000, false)
    val citylnglat = spark.read.csv("/Users/chenyang/Downloads/citydata2315.csv")
      .toDF("市", "站名", "经度", "纬度").select("市", "经度", "纬度").distinct();

    val sumfzlnglat = sumcity.join(citylnglat.toDF("发送城市", "发送城市经度", "发送城市纬度"), "发送城市")
    sumfzlnglat.show(false)

    val nameFeature = sumfzlnglat.map(row =>
      (row(0).toString, new DenseVector(Array(row(2).toString.toDouble,
        row(3).toString.toDouble, row(4).toString.toDouble))))
      .toDF("发送城市", "features")
    nameFeature.show(false)

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")


    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(nameFeature)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(nameFeature)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("发送城市", "features", "scaledFeatures").show(false)
  }

}
