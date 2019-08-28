package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 5){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath,days)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 读取字段文件
    val map = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
        .map(row=>{
          // 取出用户Id
          val userId = TagUtils.getOneUserId(row)
         // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagAPP.makeTags(row,broadcast)
        val keywordList = TagKeyWord.makeTags(row,bcstopword)
        val dvList = TagDevice.makeTags(row)
        val loactionList = TagLocation.makeTags(row)
        val business = BusinessTag.makeTags(row)
      (userId,adList++appList++keywordList++dvList++loactionList++business)
        })
      .reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      ).map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)

  }
}
