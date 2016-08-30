import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by David Rodgers on 8/25/2016
  */
object SparkWordCount {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]");
    val sc = new SparkContext(sparkConf);
    val input = sc.textFile(args(0));
    val wc = input.flatMap(line => line.split(" ")).map(word =>(word,1));
    val preOutput = wc.reduceByKey(_ + _);
    // switch key and value, sort by value, switch key and value back. sorted in ascending order
    val output = preOutput.map(item => item.swap).sortByKey(true).map(item => item.swap);
    output.saveAsTextFile(args(1));
  }

}