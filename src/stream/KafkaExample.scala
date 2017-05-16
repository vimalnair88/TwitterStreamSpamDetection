// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

package stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import net.liftweb.json._

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaExample {
  
   implicit val formats = DefaultFormats
   case class Twitter(text: String,created_at : String, id_str: String)
   
  def parser(json: String): String = {
        val parsedJson = parse(json)
        val m = parsedJson.extract[Twitter]
        return m.text
   }
   def parser1(json: String): String = {
        val parsedJson = parse(json)
        val m = parsedJson.extract[Twitter]
        return m.created_at
   }
   def parser2(json: String): String = {
        val parsedJson = parse(json)
        val m = parsedJson.extract[Twitter]
        return m.id_str
   }
  
  
  def main(args: Array[String]) {
    
  var tf = new HashingTF(numFeatures = 1000)
  val conf = new SparkConf().setAppName("tweet-spam").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val inputRdd = sc.textFile("tweets.csv")
  val linesWithSpam = inputRdd.filter(line => line.contains("spam"))
  val spam = linesWithSpam.map( x => x.split(",",2)(1))
  val linesWithHam = inputRdd.filter(line => line.contains("ham"))
  //val ham = linesWithHam.map( x => x.split(",",2)(1)) 
  val hamUpdated = linesWithHam.map(x=> x.replace(",", "")) 
  val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
  val hamFeatures = hamUpdated.map(email => tf.transform(email.split(" ")))  
  val positiveExamples = spamFeatures.map( features => LabeledPoint(1, features))
  val negativeExamples = hamFeatures.map( features => LabeledPoint(0, features))
  val training_data = positiveExamples.union(negativeExamples)
  training_data.cache()
  val Array(trainset, testset) = training_data.randomSplit(Array(0.6, 0.4))
  val lrLearner = new LogisticRegressionWithSGD()
  val model = lrLearner.run(trainset) 
  val predictionLabel = testset.map( x => (model.predict(x.features), x.label))
      
  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
  val topics = List("twitterstream").toSet
  val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)
  
  val tweets = lines.map(parser)

  //tweets.print()
  tweets.foreachRDD( rdd => {
    for(item <- rdd.collect().toArray) {
      
       val output = "Tweet :"+item+"Prediction:  "+model.predict(tf.transform(item.split(" ")))
       println (output)
    }
  }) 
  ssc.checkpoint("C:/checkpoint/")  
  ssc.start()
  ssc.awaitTermination()
  }
}

