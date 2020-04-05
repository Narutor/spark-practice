package 基础练习.test

/**
 * description
 *
 * @author 漩涡鸣人 2020/04/02 13:17
 */
object condition {
  def conditionTa(number:Long)={
    if(number!=0){
      if(number>10){
        println("!!!")
      }
      else{
        println("xxx")
      }
    }else{
      println("111")
    }

  }

  def main(args: Array[String]): Unit = {
    conditionTa(11)
  }
}
