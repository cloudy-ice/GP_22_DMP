package com.Rpt

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LocationRpt_SQL {
  def main(args: Array[String]): Unit = {
    //检查路径参数个数
    if(args.length != 1){
      println("路径错误")
      sys.exit()
    }
    //创建集合保存目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val df = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("log")
//    sQLContext.sql("select province,cityname,sum(original_req) original_req," +
//      "sum(effective_req) effective_req," +
//      "sum(ad_req) ad_req," +
//      "sum(parti_billing) parti_billing,sum(success_billing) success_billing,sum(show) show,sum(click) click," +
//      "sum(consume) consume,sum(cost) cost from " +
//      "(select requestmode,processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment,provincename,cityname " +
//      "if(requestmode == 1 && processnode >= 1, 1, 0) original_req," +
//      "if(requestmode == 1 && processnode >= 2, 1, 0) effective_req," +
//      "if(requestmode == 1 && processnode == 3, 1, 0) ad_req," +
//      "if(iseffective == 1 && isbilling ==1 && isbid ==1, 1,0) parti_billing," +
//      "if(iseffective == 1 && isbilling ==1 && iswill ==1 && adorderid != 0, 1,0) success_billing," +
//      "if(requestmode == 2 && iseffective == 1, 1,0) show," +
//      "if(requestmode == 3 && iseffective == 1, 1,0) click," +
//      "if(iseffective ==1 && isbilling ==1 && iswin ==1, winprice/1000, 0) consume," +
//      "if(iseffective ==1 && isbilling ==1 && iswin ==1, adpayment/1000, 0) cost " +
//      "from log) T " +
//      "group by provincename,cityname").show()

    sQLContext.sql("select requestmode," +
      "processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment,provincename,cityname," +
      "if(requestmode == 1 && processnode >= 1, 1, 0) original_req," +
      "if(requestmode == 1 && processnode >= 2, 1, 0) effective_req," +
      "if(requestmode == 1 && processnode == 3, 1, 0) ad_req," +
      "if(iseffective == 1 && isbilling ==1 && isbid ==1, 1,0) parti_billing," +
      "if(iseffective == 1 && isbilling ==1 && iswill ==1 && adorderid != 0, 1,0) success_billing," +
      "if(requestmode == 2 && iseffective == 1, 1,0) show," +
      "if(requestmode == 3 && iseffective == 1, 1,0) click," +
      "if(iseffective ==1 && isbilling ==1 && iswin ==1, winprice/1000, 0) consume," +
      "if(iseffective ==1 && isbilling ==1 && iswin ==1, adpayment/1000, 0) cost " +
      "from log").show()

    sc.stop()
  }

}
