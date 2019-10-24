import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object socketWindowWordCount {
  case class WordWithCount(word: String, count: Long)
  def main(args: Array[String]): Unit = {
    var hostname = "localhost"
    var port = 9090

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port = if (params.has("port")) params.getInt("post") else 9090
    } catch {
      case e: Exception => {
        System.err.print("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }

    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", port,'\n')
    val windowedCounts = text
      .flatMap {w => w.split("\\s")}
      .map{w => WordWithCount(w,1)}
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")
    windowedCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }
}
