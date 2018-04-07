/**
  * Created by pi on 17-11-23.
  */
object ShowData {
  def main(args: Array[String]) {
    val spark = new Configure().ss
//    val goodsdata = spark.read.csv("/home/pi/Documents/DataSet/Transport/goods20162.csv")
//      .toDF("id","票号","工作日期","品名","品名代码","发局","发站","到局","到站"
//        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
//    goodsdata.show(1000,false)

    val alldata = spark.read.csv("/home/pi/Documents/DataSet/Transport/alldata0114.csv")
      .toDF("id","品名","品名代码","发局","发站","到局","到站"
        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
    alldata.show(1000,false)
    val citydata1 = spark.read.csv("/home/pi/Documents/DataSet/Transport/city.csv")
      .toDF().select("_c0","_c6","_c7","_c8").toDF("站名","市","lng","lat")
    citydata1.show(false)
  }
}
