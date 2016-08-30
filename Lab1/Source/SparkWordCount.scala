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

    // word count per file
    val wc = input.flatMap(line => line.split(" ")).map(word =>(word,1));
    val preWordOutput = wc.reduceByKey(_ + _);
    // switch key and value, sort by value, switch key and value back. sorted in ascending order
    val wordOutput = preWordOutput.map(item => item.swap).sortByKey(true).map(item => item.swap);
    wordOutput.saveAsTextFile(args(1));

    // word count per sentence
    val sentCount = input.flatMap(line => line.split('.')).map(sentence => (sentence, (sentence.split(" ")).length));
    val preSentOutput = sentCount.reduceByKey(_+_);
    // switch key and value, sort by value, switch key and value back. sorted in ascending order
    val sentOutput = preSentOutput.map(item => item.swap).sortByKey(true).map(item => item.swap);
    sentOutput.saveAsTextFile(args(2));
  }

}