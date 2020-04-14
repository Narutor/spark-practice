package 基础练习.exception

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/10 12:55
 */
object Exception {
  def main(args: Array[String]): Unit = {


    try {
      val al = 1
      val b = 0
      println(al / b)
    } catch {
      case e =>println(e)
    }


  }
}
