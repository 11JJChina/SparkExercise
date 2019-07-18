package com.zhang.util

import scala.util.matching.Regex

object AccessLogUtil {

  /**
    * 日志数据格式:
    * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
    *
    * @param ipAddress    IP地址
    * @param clientId     客户端唯一标识符
    * @param userId       用户唯一标识符
    * @param serverTime   服务器时间
    * @param method       请求类型/方式
    * @param endpoint     请求的资源
    * @param protocol     请求的协议名称
    * @param responseCode 请求返回值：比如：200、401
    * @param contentSize  返回的结果数据大小
    */
  case class AccessLog(
                        ipAddress: String,
                        clientId: String,
                        userId: String,
                        serverTime: String,
                        method: String,
                        endpoint: String,
                        protocol: String,
                        responseCode: Int,
                        contentSize: Long
                      )

  val PARTTERN: Regex =
    """
      ^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)
    """.r

  /**
    * 校验日志格式是否符合规则
    *
    * @param line
    * @return
    */
  def isValidateLogLine(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)

    if (options.isEmpty) {
      false
    } else {
      true
    }
  }

  /**
    * 解析输入的日志数据
    *
    * @param line
    * @return
    */
  def parseLogLine(line: String): AccessLog = {
    if (!isValidateLogLine(line)) {
      throw new IllegalArgumentException("参数格式异常")
    }

    // 从line中获取匹配的数据
    val options = PARTTERN.findFirstMatchIn(line)

    // 获取matcher
    val matcher = options.get

    // 构建返回值
    AccessLog(
      matcher.group(1), // 获取匹配字符串中第一个小括号中的值
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8).toInt,
      matcher.group(9).toLong
    )
  }

  /**
    * 自定义的一个二元组的比较器
    */
  object TupleOrdering extends scala.math.Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      // 按照出现的次数进行比较，也就是按照二元组的第二个元素进行比较
      x._2.compare(y._2)
    }
  }


}
