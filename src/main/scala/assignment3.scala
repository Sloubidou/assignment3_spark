/* assignment3.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext

object SimpleApp {

	case class Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)

	def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
		num.toDouble( ts.sum ) / ts.size
	}

  	def main(args: Array[String]) {

	    val conf = new SparkConf().setAppName("Assignment3")
	    val sc = new SparkContext(conf)

		//case class Crimes(cdatetime:String,address:String,district:String,beat:String,grid:String,crimedescr:String,code:String,latitude:String,longitude:String)
	    val file = sc.textFile("crimes.csv") //path of the file for me
	    //we read the crimes file and delete the header line
	    val crimes = file.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
	  	

	  	// we create a rdd containing the class filled with the crimes file without the header
	 	val crimesClass = crimes.map(line => { 
	 		val l = line.split(",") 
	 		Crimes(l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8))
	 		})


	 	/***************************** First question RDD**************************/
	 	//we create a pair with the class crimedescription element
	 	val pairsCrimeType = crimesClass.map(l => (l.crimedescr,1))
	 	//we reduce it to create a map containing (crimedescription, number of crimes for the descrition)
	 	val countsType = pairsCrimeType.reduceByKey((a, b) => a + b)
	 	//We choose the max with an order
	 	val max = countsType.max()(new Ordering[Tuple2[String, Int]]() {
		  override def compare(x: (String, Int), y: (String, Int)): Int = 
		      Ordering[Int].compare(x._2, y._2)
		})
	 	println(s"Crime that happens the most in Sacramento : $max")

	 	/***************************** Second question RDD*************************/
	 	//We create a pair with the class where we isolat the date from the cdatetime column and match it with a 1 we get (date, 1)
	 	val pairsCrimeDays = crimesClass.map( line => { 
	 		val tutu = line.cdatetime.split(" ") 
	 		(tutu(0), 1) 
	 		})
	 	//We reduce it to create  map containing (date, number of crimes at the date)
	 	val countsDays = pairsCrimeDays.reduceByKey((a, b) => a + b)
	 	//We get the top 3 number of the countsDay in the desc order
	 	val ordered = countsDays.top(3)(new Ordering[Tuple2[String, Int]]() {
		  override def compare(x: (String, Int), y: (String, Int)): Int = 
		      Ordering[Int].compare(x._2, y._2)
		})
	 	ordered.foreach(println)

	 	/***************************** Third question RDD*************************/

	 	//we reuse the reduced map of the first question (crimedescription, number of crimes for the descrition) and divide the number of crime by 31, number of days in january
	 	val meanCrimeTypeDay = countsType.map(line => (line._1, line._2.toFloat/31))
	 	meanCrimeTypeDay.foreach(println)


	 	/******************************Part Two **********************************/


	 	val pairsCrimeDist= crimesClass.map( line => (line.district, 1))
	 	val countsDist = pairsCrimeDist.reduceByKey((a, b) => a + b)
	 	val crimeDistDay = countsDist.map(line => (line._1, line._2.toFloat/31))
		val output = crimeDistDay.map( line => Array(line._1, line._2).mkString(","))

		//val blabla = crimeDistDay.map{(key,value) => Array(key, value).mkString(",")}
		output.saveAsTextFile("output.txt")



  }
}