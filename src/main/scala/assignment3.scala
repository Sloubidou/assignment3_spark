/* assignment3.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Assignment3")
    val sc = new SparkContext(conf)

	case class Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)
    val file = sc.textFile("crimes.csv")
    val crimes = file.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  	
 	val crimesClass = file.map(line => { 
 		val l = line.split(",") 
 		Crimes(l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8))
 		})

 	val pairsCrimeType = crimesClass.map(l => (l.code,1))
 	val counts = pairsCrimeType.reduceByKey((a, b) => a + b)
 	counts.foreach(println)
  }
}


// case class Crimes(cdatetime:String,address:String,district:Int,beat:String,grid:Int,crimedescr:String,ucr_ncic_code:Int,latitude:Float,longitude:Float)
// val file = sc.textFile("crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
// val crimes = file.map(line => { val l = line.split(";") Crimes(l(0), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6), l(7).toInt, l(8).toFloat, l(9).toFloat)})