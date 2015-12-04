package org.yurshina

case class Log(ipAddress: String, bytes: Long, browser: String)

class Parser extends java.io.Serializable {

  def parseString(line: String) : Log = {
    val tokens = line.split("\\s+")
    try {
      new Log(tokens(0), if (tokens.length < 10) 0L else tokens(9).toLong, if (tokens.length > 11) tokens(11) else "")
    } catch {
      case e : NumberFormatException => new Log("", 0L, "")
    }
  }
}