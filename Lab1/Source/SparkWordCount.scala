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

    // count # of sentences
    val sentCount = input.flatMap(line => line.split('.')).map(sentence => (sentence, 1);
    val preSentOutput = sentCount.reduceByKey(_+_);
    // sort alphabetically
    val sentOutput = preSentOutput.sortByKey(true);
    sentOutput.saveAsTextFile(args(1));
  }

}