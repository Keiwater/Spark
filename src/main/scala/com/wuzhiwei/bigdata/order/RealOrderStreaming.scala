package com.wuzhiwei.bigdata.order

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.wuzhiwei.bigdata.jedis.JedisPoolUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * 从Kafka Topic中获取数据，实时统计分析订单数据（窗口统计Top5订单量和累加订单销售额）
  */
object RealOrderStreaming {

  // 日志打印设置：
  private val logger: Logger = Logger.getLogger(RealOrderStreaming.getClass)

  // 设置检查点目录
  val CHECK_POINT_PATH = "E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\order\\orderCK00010"
  // 设置SparkStreaming 应用的 batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)
  // 设置：窗口时间间隔
  val STREAING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 3
  // 设置滑动时间窗口
  val STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 2

  // 存储Redis 中实时统计销售额 结果( 结果表名称 )
  /**
    *
    * 定义： Redis Hash 对象名称。用来存储计算累计结果：
    *
    */
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "orders:total:price"


  /**
    * 使用scala 贷出模式定义函数，管理StreaminContext流式上下文对象的创建与管理
    * @param operation
    *
    * @param args
    */
  def sparkOperation(args: Array[String])(operation:StreamingContext => Unit): Unit ={

    val creatingFunc :() => StreamingContext = () => {

      val conf = new SparkConf()
        .setAppName("RealOrderStreaming")
        .setMaster("local[*]")
      // 每秒钟获取Topic 中 每个分区的最大条目数，比如设置 10000条，Topic三个分区，每秒钟最大数据量为30000，批处理时间间隔为5 秒，则每批次处理数据量最多为 15000：
        .set("spark.streaming.kafka.maxRatePerPartiton","30000")
      //  设置数据处理本地性等待超时时间
        .set("spark.locality.wait","100ms")
      // 设置使用Kryo 序列化，针对非基础数据类型 ，需要进行数据类型：类 注册
        .set("spark.serilaizer","org.apache.spark.serializer.Kryoserializer")
        .registerKryoClasses(Array(classOf[SaleOrder]))
      // 设置启用反压机制：
        .set("spark.streaming.backpressure.enable","true")
      // 设置Driver JVM 的 GC 策略：
        .set("spark.driver.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC")

      val ssc: StreamingContext = new StreamingContext(conf, STREAMING_BATCH_INTERVAL)
      ssc.checkpoint(CHECK_POINT_PATH)
      ssc.sparkContext.setLogLevel("WARN")

      operation(ssc)

      ssc

    }

    // 闯将StreamingContext实例对象
    var context:StreamingContext = null
    try{
      context = StreamingContext.getActiveOrCreate(CHECK_POINT_PATH,creatingFunc)

      // 启动实时应用； 等待实时应用的终止
      context.sparkContext.setLogLevel("WARN")
      context.start()
      context.awaitTermination()
      context.stop(stopSparkContext = true, stopGracefully = true)

    }catch{
      case e : Exception => e.printStackTrace()
    }finally {
      if(context != null){
        context.stop(stopSparkContext = true, stopGracefully = true)
      }
    }

  }


  /**
    * 贷出模式中的用户函数：实时获取kafka Topic中的数据，依据业务分析统计
    *
    * @param ssc
    */
  def processStreamingData(ssc: StreamingContext): Unit = {

    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> ConstantUtils.METADATA_BROKER_LIST,
      "auto.offset.reset" -> ConstantUtils.AUTO_OFFSET_RESET
    )

    val topics: Set[String] = Set(ConstantUtils.ORDER_TOPIC)

    // 从  kafka 读取流式实时数据
    /** ===================   a.  从kafka Topic中，读取数据，使用Direct 方式：   ================================ */
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )

    //    kafkaDStream.print()

    /** ===================   b.  解析Message 数据为SaleOrder 格式的数据   ================================ */
    val orderDStream: DStream[(Int, SaleOrder)] = kafkaDStream.transform( rdd => {
      rdd.mapPartitions( iter => {
        iter.map { case (_, message) =>
          val mapper: ObjectMapper = ObjectMapperSingleton.getInstance()
          // 解析JSON 格式的数据为SaleOrder对象
          val saleOrder: SaleOrder = mapper.readValue(message, classOf[SaleOrder])
          //返回结果：
          (saleOrder.provinceId, saleOrder)
      }
     })
   })


    // orderDStream.print(10)

    /** ===================   c.处理订单数据：实时累加统计各个省份销售额： ================================ */

    val orderPriceDStream: DStream[(Int, Float)] = orderDStream.updateStateByKey(
        (batchTime: Time, provinceId: Int, orders: Seq[SaleOrder], state: Option[Float]) => {
          val currentOrderPrice: Float = orders.map(_.orderPrice).sum
          val previousOrderPrice: Float = state.getOrElse(0.0f)
          Some(previousOrderPrice + currentOrderPrice)
        },
          new HashPartitioner(ssc.sparkContext.defaultMinPartitions),
          rememberPartitioner = true
    )


    // orderPriceDStream.print(10)


    /** ===================   d.将实时统计分析各个省份的销售订单额存储Redis中：================================ */
    orderPriceDStream.foreachRDD((rdd,time) => {

        // 处理batchTime
        val batchTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
        logger.warn("--------------------------------------------------")
        logger.warn(s"batchTime: $batchTime")
        logger.warn("--------------------------------------------------")

        if(!rdd.isEmpty()){
          // 降低分区：
          rdd.coalesce(1).foreachPartition(iter => {
            // TODO : 获取Jedis 连接
            val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
            // TODO: 将统计销售订单结果保存到Redis 中：
            iter.foreach{
              case (provinceId, orderTotal) =>
              jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, orderTotal.toString )
              logger.warn(s"==========  provinceId: $provinceId , orderTotal : $orderTotal ===========")
            }

            // 关闭Jedis：
            JedisPoolUtil.release(jedis)
          })
        }
    })


     /** ===================   e.将实时统计最近时间段内订单量最高的前十个省份 (窗口统计)：================================ */
     /**
       * 设计窗口大小，
       * 可滑动的大小
       *
       */

    // 连接数据属性信息：
    val (url, props) = {

      // 连接数据库的URL
      val jdbcUrl = "jdbc:mysql://localhost:3306/"

      // JDBC database connection arguments, 至少包含连接数据库的用户名，密码
      val jdbcProps : Properties = new Properties()
      jdbcProps.put("user","root")
      jdbcProps.put("password","123")

      // 返回数据库连接信息：
      ( jdbcUrl, jdbcProps)

    }




    orderDStream
      .window(STREAING_WINDOW_INTERVAL, STREAMING_SLIDER_INTERVAL)
      .foreachRDD((rdd,time) => {

        // 时间处理：
        val batchTime: String = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
        logger.warn(s"batchTime: $batchTime")
        if(!rdd.isEmpty()){

          // RDD 数据缓存
          rdd.cache()

          // TODO: i.构建SparkSession 实例对象
          val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).config("spark.sql.shuffle.","6").getOrCreate()

          import spark.implicits._

          val saleOrderDF: DataFrame = rdd.map(_._2).toDF()

          val top10ProvinceOrderCountDF: Dataset[Row] = saleOrderDF.groupBy($"provinceId").count().orderBy($"count".desc).limit(10)

          // 将数据写入到 Mysql 表中去；
          top10ProvinceOrderCountDF.write.mode(SaveMode.Overwrite).jdbc(url, "test.order_top10_count", props)

          // 释放RDD 数据
          rdd.unpersist()

        }



  })

    // 结合业务分析需求，对数据进行加工

    // 结果输出

}


  def main(args: Array[String]): Unit = {


    sparkOperation(args)(processStreamingData)

}
}
