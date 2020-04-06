import java.net.URL

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Description：xxx源程序<br/>
 * Copyright (c) ，2019 ， xu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2020年04月06日
 *
 * @author 徐威
 * @version : 1.0
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1.初始化批计算环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4) //设置并行度

    // 2. 导入隐式转换
    import org.apache.flink.api.scala._

    // 3. 读取数据
    val dataPath: URL = getClass.getResource("/wc.txt")
    val data: DataSet[String] = env.readTextFile(dataPath.getPath)

    // 4. 转换和处理数据
    data.flatMap(_.split("\\s+"))
        .map((_,1))
        .groupBy(0)
        .sum(1)
        .print()
  }

}
