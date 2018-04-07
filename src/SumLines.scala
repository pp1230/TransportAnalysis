/**
  * Created by pi on 18-4-2.
  */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object SumLines {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val filtereddata = spark.read.csv("/home/pi/Documents/DataSet/Transport/FilteredData.csv")
        .toDF("品名","品名代码","发送城市", "发送城市经度", "发送城市纬度", "到达城市", "到达城市经度", "到达城市纬度",
          "物流总包（Y物流总包，N非物流总包）","车数","吨数")

    //filtereddata.show(10000)
    //println("LinesNum:"+filtereddata.count())
    val sumcity = filtereddata.select(
      filtereddata.col("发送城市"), filtereddata.col("到达城市"),filtereddata.col("吨数").cast(DoubleType))
      .groupBy("发送城市", "到达城市").sum("吨数")
      sumcity.orderBy(desc("sum(吨数)")).show(10000, false)
    sumcity.orderBy("发送城市").show(10000, false)
    println("LinesNum:"+sumcity.count())

    // sumcity.filter($"sum(吨数)" > 300000).orderBy(desc("sum(吨数)")).show(100);
    //filtereddata.filter($"品名" === "玉米芯粉" && $"发送城市" === "平顶山"&& $"到达城市" === "什邡").show(1000, false)
    //filtereddata.orderBy("物流总包（Y物流总包，N非物流总包）").show(1000,false)
  }
}
