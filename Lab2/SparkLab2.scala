import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by David Rodgers on 01-Sep-16.
  */
object SparkLab2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkActions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val inputData = sc.textFile("C:\\Users\\DR042460\\Documents\\RT_BigData_Fall2016_Rodgers\\Lab2\\input.txt")
    // 1st transformation - find length of each line
    val linesLength = inputData.map(L => L.length)
    // 2nd transformation - filter out lengths that are less than or equal to 10 characters
    val filterLines = linesLength.filter(F => F > 10)
    // 1st action - take 5 random line length values without replacement
    val sampleLines = filterLines.takeSample(false, 5)
    // 2nd action - reduce the sample values to find a grand total
    val collectSampleLines = sc.parallelize(sampleLines)
    val totalValue = collectSampleLines.reduce((x, y) => (x + y))

    // output each step
    linesLength.saveAsTextFile("C:\\Users\\DR042460\\Documents\\RT_BigData_Fall2016_Rodgers\\Lab2\\Transformation-1-GetLengths")
    filterLines.saveAsTextFile("C:\\Users\\DR042460\\Documents\\RT_BigData_Fall2016_Rodgers\\Lab2\\Transformation-2-FilterValues")
    collectSampleLines.saveAsTextFile("C:\\Users\\DR042460\\Documents\\RT_BigData_Fall2016_Rodgers\\Lab2\\Action-1-SampleValues")
    val outputTotalValue = sc.parallelize(Array(totalValue))
    outputTotalValue.saveAsTextFile("C:\\Users\\DR042460\\Documents\\RT_BigData_Fall2016_Rodgers\\Lab2\\Action-2-CalculateTotal")
  }
}







