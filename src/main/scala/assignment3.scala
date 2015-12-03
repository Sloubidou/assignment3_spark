/* assignment3.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.DoubleRDDFunctions

object SimpleApp {

	case class Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Assignment3")
    val sc = new SparkContext(conf)

	//case class Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)
    val file = sc.textFile("crimes.csv")
    val crimes = file.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  	
 	val crimesClass = file.map(line => { 
 		val l = line.split(",") 
 		Crimes(l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8))
 		})

 	/***************************** First question RDD**************************/
 	val pairsCrimeType = crimesClass.map(l => (l.crimedescr,1))
 	val countsType = pairsCrimeType.reduceByKey((a, b) => a + b)
 	//countsType.foreach(println)
 	val max = countsType.max()(new Ordering[Tuple2[String, Int]]() {
	  override def compare(x: (String, Int), y: (String, Int)): Int = 
	      Ordering[Int].compare(x._2, y._2)
	})
 	println(s"Crime that happens the most in Sacramento : $max")

 	/***************************** Second question RDD*************************/
 	val pairsCrimeDays = crimesClass.map( line => { 
 		val tutu = line.cdatetime.split(" ") 
 		(tutu(0), 1) 
 		})
 	val countsDays = pairsCrimeDays.reduceByKey((a, b) => a + b)
 	val ordered = countsDays.top(3)(new Ordering[Tuple2[String, Int]]() {
	  override def compare(x: (String, Int), y: (String, Int)): Int = 
	      Ordering[Int].compare(x._2, y._2)
	})
 	ordered.foreach(println)

 	/***************************** Third question RDD*************************/
/*
 	val values = countsDays.map(line => line._2).mean()
 	println(s"MOYENNE CRIMES PAR JOUR: $values")*/

 	val values = countsDays.values
 	val mean = values.mean
 	println(s"Average crimes : $mean")


  }
}


// case class Crimes(cdatetime:String,address:String,district:Int,beat:String,grid:Int,crimedescr:String,ucr_ncic_code:Int,latitude:Float,longitude:Float)
// val file = sc.textFile("crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
// val crimes = file.map(line => { val l = line.split(";") Crimes(l(0), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6), l(7).toInt, l(8).toFloat, l(9).toFloat)})