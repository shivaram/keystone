package pipelines.text

import evaluation.{BinaryClassifierEvaluator, MulticlassClassifierEvaluator}
import loaders.{AmazonReviewsDataLoader, NewsgroupsDataLoader}
import nodes.learning.NaiveBayesEstimator
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.{CommonSparseFeatures, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser
import workflow.Optimizer

object AmazonReviewsPipeline extends Logging {
  val appName = "AmazonReviewsPipeline"

  def run(sc: SparkContext, conf: AmazonReviewsConfig) {

    val trainData = AmazonReviewsDataLoader(sc, conf.trainLocation, conf.threshold)
    trainData.labeledData.cache()

    val training = trainData.data.cache()
    val labels = trainData.labels.cache()

    // Build the classifier estimator
    logInfo("Training classifier")
    val predictorPipeline = Trim andThen LowerCase() andThen
      Tokenizer() andThen
      NGramsFeaturizer(1 to conf.nGrams) andThen
      TermFrequency(x => 1) andThen
      (CommonSparseFeatures(conf.commonFeatures), training) andThen
      (NaiveBayesEstimator(2), training, labels) andThen
      MaxClassifier


    val predictor = Optimizer.execute(predictorPipeline)
    logInfo("\n" + predictor.toDOTString)

    // Evaluate the classifier
    logInfo("Evaluating classifier")

    val testData = AmazonReviewsDataLoader(sc, conf.testLocation, conf.threshold)
    val testLabels = testData.labels
    val testResults = predictor(testData.data)
    val eval = BinaryClassifierEvaluator(testResults.map(_ > 0), testLabels.map(_ > 0))

    logInfo("\n" + eval.summary())
  }

  case class AmazonReviewsConfig(
                               trainLocation: String = "",
                               testLocation: String = "",
                               threshold: Double = 3.5,
                               nGrams: Int = 2,
                               commonFeatures: Int = 100000)

  def parse(args: Array[String]): AmazonReviewsConfig = new OptionParser[AmazonReviewsConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Double]("threshold") action { (x,c) => c.copy(threshold=x)}
    opt[Int]("nGrams") action { (x,c) => c.copy(nGrams=x) }
    opt[Int]("commonFeatures") action { (x,c) => c.copy(commonFeatures=x) }
  }.parse(args, AmazonReviewsConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }

}
