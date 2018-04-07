import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType}

/**
  * Created by pi on 18-1-14.
  */
object AnalysisAlldata {
  def main(args: Array[String]) {
    val spark = new Configure().ss
    import spark.implicits._
    val alldata = spark.read.csv("/home/pi/Documents/DataSet/Transport/alldata0114.csv")
      .toDF("id","品名","品名代码","发局","发站","到局","到站"
        ,"发货人","物流总包（Y物流总包，N非物流总包）","车数","吨数","收入")
    //alldata.show(false)
    val transformfz = alldata.select(alldata.col("id"),
      regexp_replace(alldata.col("发站"), "['东','西','南','北','境']$","").name("站名")
      ,alldata.col("到站"))
    //transformfz.show(100,false)
    val fz = alldata.join(transformfz, "id")
    fz.show(100,false)

    val transformdz = alldata.select(alldata.col("id"),
      alldata.col("发站")
      ,regexp_replace(alldata.col("到站"), "['东','西','南','北','境']$","").name("站名"))
    //transformdz.show(100,false)
    val dz = alldata.join(transformdz, "id")
    dz.show(100,false)

    /**
      * 计算站点对应的经维度和市
      */
//    val citydata1 = spark.read.csv("/home/pi/Documents/DataSet/Transport/city.csv")
//      .toDF().select("_c0","_c6","_c7","_c8").toDF("站名","市","lng","lat")
//    val citydata2 = citydata1.select(regexp_replace(citydata1.col("站名"), "['站']$","").name("站名")
//      , citydata1.col("市"), citydata1.col("lng"), citydata1.col("lat"))
//    //citydata2.show(1000)
//    println("--------全国全部站点经纬度--------")
//    println("CountryCount:"+citydata2.count())
//
//    val checkcity1 = spark.read.csv("/home/pi/Documents/DataSet/Transport/checkcity.csv")
//        .toDF("站名", "局", "市").select("站名", "市")
//    println("DataCount1:"+checkcity1.count())
//    val checkcity2 = checkcity1.filter(checkcity1.col("市").isNotNull)
//        checkcity2.show(100)
//    println("--------铁路数据全部站点--------")
//    println("DataCount2:"+checkcity2.count())
//
//    val addcity = spark.read.csv("/home/pi/Documents/DataSet/Transport/addlnglat.csv")
//      .toDF("市","lat","lng").select("市","lng","lat")
//    val citydata3 = citydata2.select("市","lng","lat").groupBy("市").agg(avg("lng"), avg("lat"))
//      .toDF("市","lng","lat").union(addcity)
//    citydata3.show(1000)
//    val citydata4 = checkcity2.join(citydata3, "市")
//        citydata4.show(100)
////    val checkdata = checkcity2.join(citydata3, Seq("市"), "outer").filter($"lat".isNull)
////      .select("市", "lat", "lng").distinct()
////    checkdata.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/checkcitydata")
////    println("CheckCount:"+checkdata.count())
////    checkdata.show(1000)
//    println("--------铁路数据全部站点经纬度--------")
//    println("DataCount3:"+citydata4.count())
//    citydata4.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/citydata2315")
//
//    val citydata5 = citydata2.union(citydata4)
//    citydata5.show(100)
//    println("--------全国全部站点及其经纬度--------")
//    println("AllCount:"+citydata5.count())

//    val getfz = fz.join(citydata2, Seq("站名", "站名"), "outer")
//    getfz.show()
//    val resultfz = getfz.select("发站", "发局", "市").filter(getfz.col("发站").isNotNull)
//      .distinct()
//    resultfz.show(1000,false)
//
//    val getdz = dz.join(citydata2, Seq("站名", "站名"), "outer")
//    getdz.show()
//    val resultdz = getdz.select("到站", "到局", "市").filter(getdz.col("到站").isNotNull)
//      .distinct()
//    resultdz.show(1000,false)
//
//    val all = resultdz.toDF("站","局","市").union(resultfz.toDF("站","局","市")).distinct()
//      .orderBy("市")
//    all.show(10000,false)
//    all.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/check")


    //获得车站经维度
    val citylnglat = spark.read.csv("/home/pi/Documents/DataSet/Transport/output/citydata2315")
      .toDF("市", "站名", "lng", "lat");
    val joinfz = fz.join(citylnglat,"站名")
    joinfz.show()
    //joinfz.select("站名", "车数", "吨数", "收入", "lng", "lat").repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/alldataitemsfz2092")
    val joindz = dz.join(citylnglat,"站名")
    joindz.show()
    //joindz.select("站名", "车数", "吨数", "收入", "lng", "lat").repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/alldataitemsdz2092")


//    val citylnglat = citydata1.select(
//      citydata1.col("市"),citydata1.col("lng").cast(DoubleType),citydata1.col("lat").cast(DoubleType))
//      .groupBy("市").avg("lng","lat")


    val fzsum = joinfz
      .select(
      joinfz.col("市").cast(StringType), joinfz.col("车数").cast(IntegerType),
      joinfz.col("吨数").cast(DoubleType),joinfz.col("收入").cast(DoubleType),
        joinfz.col("lng").cast(DoubleType),joinfz.col("lat").cast(DoubleType)
      )
      .groupBy("市","lng","lat").sum("车数","吨数","收入")
      //.join(citylnglat,"市")
      .orderBy(desc("sum(吨数)"))
    fzsum.show()
    println("Count:"+fzsum.count())
    //fzsum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/alldatafzsum2315")

    val dzsum = joindz.select(
      joindz.col("市").cast(StringType), joindz.col("车数").cast(IntegerType),
      joindz.col("吨数").cast(DoubleType),joindz.col("收入").cast(DoubleType),
      joinfz.col("lng").cast(DoubleType),joinfz.col("lat").cast(DoubleType)
      )
      .groupBy("市","lng","lat").sum("车数","吨数","收入")
      //.join(citylnglat,"市")
      .orderBy(desc("sum(吨数)"))
    dzsum.show()
    //dzsum.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/alldatadzsum2315")

//
//    val fzdzsum = fzsum.union(dzsum).orderBy("市")
//    fzdzsum.show()
//    val fzdzgroup = fzdzsum.groupBy("市").agg(sum(fzdzsum("sum(车数)")),sum(fzdzsum("sum(吨数)")),sum(fzdzsum("sum(收入)")),
//      avg(fzdzsum("avg(lng)")),avg(fzdzsum("avg(lat)")))
//    fzdzgroup.show()
//    fzdzgroup.repartition(1).write.csv("/home/pi/Documents/DataSet/Transport/output/alldatafzdzsum")
  }
}
