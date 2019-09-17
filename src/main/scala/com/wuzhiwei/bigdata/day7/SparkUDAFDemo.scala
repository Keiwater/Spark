package com.wuzhiwei.bigdata.day7

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object SparkUDAFDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkUDAFDemo").master("local[*]").getOrCreate()

     val df: Dataset[lang.Long] = spark.range(1,10)

    df.show()


    val gm = new GeometricMean

    spark.udf.register("gm",gm)


    // 方法一：
//    df.createTempView("v_GeoNum")
//
//    val res: DataFrame = spark.sql("select gm(id) from v_GeoNum ")
//
//    res.show()


    // 方法二、三：
    // df.groupBy().agg(expr("gm(id)")).as("dd").show
    df.select(expr("gm(id) as GeometricMean")).show


  }
}


// 实现，几何平均数  UDAF 计算逻辑
class GeometricMean extends UserDefinedAggregateFunction {

  // 输入的数据格式
  override def inputSchema: StructType = StructType(List(StructField("value",DoubleType)))


  // 缓存临时结果的数据格式
  override def bufferSchema: StructType = StructType(List(
    // 入参数据集的个数（行数）
    StructField("count",LongType),
    //  存储加工结果
    StructField("product",DoubleType)

  ))

  // 返回的结果数据类型
  override def dataType: DataType = DoubleType

  // 幂等性 多次执行 相同
  override def deterministic: Boolean = true

  // 初始化  一个保留个数 一个保留乘积
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) = 0L
    buffer(1) = 1.0

  }

  //  更新 数据个数加1    与之前数据相乘
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1)*input.getAs[Double](0)

  }

  // 缓存数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0)=buffer1.getAs[Long](0)+buffer2.getAs[Long](0)

    buffer1(1)=buffer1.getAs[Double](1)*buffer2.getAs[Double](1)
  }


  // 缓存数据合并
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1),1.toDouble/buffer.getLong(0))
  }
}



object TestGM {

  def main(args: Array[String]): Unit = {

    val r = Math.pow(1*2*3*4*5*6*7*8*9, 1.toDouble/9)
    print(r)


  }


}