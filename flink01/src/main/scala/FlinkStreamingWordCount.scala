import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Description：xxx源程序<br/>
 * Copyright (c) ，2019 ， xu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2020年04月06日
 *
 * @author 徐威
 * @version : 1.0
 */
object FlinkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 1.初始化流计算环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(4) //设置并行度


    // 2. 导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 3. 读取数据
    val stream: DataStream[String] = streamEnv.socketTextStream("xw1", 6666) // 设置端口和主机

    // 4. 转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 5. 打印结果
    result.print("res:")

    // 6. 启动流计算程序
    streamEnv.execute("myWordCount")

  }
}
