package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class WordCount {

  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

  public static void main(String[] args) {
    checkArgument(args.length > 0, "Please provide the path of input file as first parameter.");
    new WordCountTask().run(args[0]);
  }

  /**
   * The task body
   */
  public void run(String inputFilePath) {

    String master = "local[*]";

    SparkConf conf = new SparkConf()
        .setAppName(WordCountTask.class.getName())
        .setMaster(master);
    JavaSparkContext context = new JavaSparkContext(conf);
    
    context.textFile(inputFilePath)
        .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .foreach(result -> LOGGER.info(
            String.format("Word [%s] count [%d].", result._1(), result._2)));
  }
}
