package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext3 {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    //d:\output dir/result dir/app_dict.txt dir/stopwords.txt
    val Array(inputPath,outputPath,dirPath,stopPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // todo 调用Hbase API
    // 加载配置文件
//    val load = ConfigFactory.load()
////    val hbaseTableName = load.getString("hbase.TableName")
//    // 创建Hadoop任务
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
//    // 创建HbaseConnection
//    val hbconn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbconn.getAdmin
//    // 判断表是否可用
//    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
//      // 创建表操作
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      val descriptor = new HColumnDescriptor("tags")
//      tableDescriptor.addFamily(descriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbconn.close()
//    }
//    // 创建JobConf
//    val jobconf = new JobConf(configuration)
//    // 指定输出类型和表
//    jobconf.setOutputFormat(classOf[TableOutputFormat])
//    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

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
    val result = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
        .map(row=> {
      // 取出用户Id
      val userId = TagUtils.getAllUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）
      (userId, row)
    })
    val vre = result.flatMap(tp => {
      val row = tp._2
        val adList = TagsAd.makeTags(row)
        val appList = TagAPP.makeTags(row,broadcast)
        val keywordList = TagKeyWord.makeTags(row,bcstopword)
        val dvList = TagDevice.makeTags(row)
        val loactionList = TagLocation.makeTags(row)
//        val business = BusinessTag.makeTags(row)
       val currentRowTag = adList++appList++keywordList++dvList++loactionList
      // List[String]    List[(String.Int)]
      val VD = tp._1.map((_,0)) ++ currentRowTag
      // 只有第一个人可以携带顶点VD，其他不需要
      // 如果同一行上的多个顶点VD ， 因为同一行数据都一个用户
      // 将来肯定要聚合在一起的，这样就会造成重复的叠加了
      tp._1.map(uId=>{
        if(tp._1.head.equals(uId)){
          (uId.hashCode.toLong,VD)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })//.take(50).foreach(println)
    //构建边的集合
    val edges = result.flatMap(tp => {
      tp._1.map(uid => {
        Edge(tp._1.head.hashCode.toLong,uid.hashCode.toLong,0)
      })
    })
    //edges.take(21).foreach(println)
    //图计算
    val graph = Graph(vre,edges)
    //调用连通图算法，找到图中可以连通的分支
    //并取出每个连通分支中最小的点的元组集合
    val cc = graph.connectedComponents().vertices
    //cc.take(20).foreach(println)
    // 认祖归宗
    cc.join(vre).map{
      case (uid,(commonId,tagsAndUserid))=>(commonId,tagsAndUserid)
    }.reduceByKey{
      case (list1,list2)=>
        (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }.take(20).foreach(println)

    sc.stop()
  }
}
