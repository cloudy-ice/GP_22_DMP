package com.Test1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val log = sc.textFile("dir/Json.txt")
    val logs = log.collect().toBuffer
    var list: List[List[String]] = List()

    for(i <- 0 until logs.length){
      val str: String = logs(i).toString
      val jsonparse = JSON.parseObject(str)

      val status = jsonparse.getIntValue("status")
      if(status == 0 ) return ""

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return null

      //创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()
      //循环输出
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val list1:List[String] = buffer.toList

      list :+= list1
    }

    val res1 = list.flatMap(x => x)
      .filter(x => x != "[]")
      .map(x => (x,1))
        .groupBy(x => x._1)
        .mapValues(x => x.size).toList.sortBy(x => x._2)

    res1.foreach(println)
    sc.stop()
  }

}
