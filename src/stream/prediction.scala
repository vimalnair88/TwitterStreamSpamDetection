package stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

object SpamDetection extends App {
  val conf = new SparkConf().setAppName("tweet-spam").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val inputRdd = sc.textFile("tweets.csv")
  val linesWithSpam = inputRdd.filter(line => line.contains("spam"))
  val spam = linesWithSpam.map( x => x.split(",",2)(1))
  val linesWithHam = inputRdd.filter(line => line.contains("ham"))
  print (linesWithHam)
  //val ham = linesWithHam.map( x => x.split(",",2)(1)) 
  val hamUpdated = linesWithHam.map(x=> x.replace(",", ""))
  var tf = new HashingTF(numFeatures = 1000)
  
  val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
  val hamFeatures = hamUpdated.map(email => tf.transform(email.split(" ")))
  
  val positiveExamples = spamFeatures.map( features => LabeledPoint(1, features))
  val negativeExamples = hamFeatures.map( features => LabeledPoint(0, features))
  val training_data = positiveExamples.union(negativeExamples)
  
  //training_data.collect().foreach(println)
  
  training_data.cache()
  val Array(trainset, testset) = training_data.randomSplit(Array(0.6, 0.4))
  val lrLearner = new LogisticRegressionWithSGD()
  val model = lrLearner.run(trainset)

  val predictionLabel = testset.map( x => (model.predict(x.features), x.label))

  val accuracy = predictionLabel.filter(r => r._1 == r._2).count.toDouble / testset.count

  print ("Accuracy: "+accuracy)
}