package pipelines.solvers

import scopt.OptionParser
import breeze.linalg._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import pipelines.Logging

import loaders.CsvFileDataLoader
import loaders.CsvDataLoader
import nodes.learning._
import nodes.util.TopKClassifier
import utils.Stats

object BlockWeightedSolver extends Serializable with Logging {
  val appName = "BlockWeightedSolver"

  def run(sc: SparkContext, conf: BlockWeightedSolverConfig) {
    // Load the data

    // val trainingFeatures = CsvDataLoader(
    //   sc,
    //   conf.trainFeaturesDir, 1600, " ").cache().setName("trainFeatures")
    // val trainingLabels = CsvDataLoader(
    //   sc,
    //   conf.trainLabelsDir, 1600, " ").map {x => 
    //     val out = Array.fill(1000)(-1.0)
    //     out(x(0).toInt - 1) = 1.0
    //     DenseVector(out)
    //   }.cache().setName("trainLabels")

    val trainLabelFeatures = sc.textFile(conf.trainFeaturesDir).map { row =>
      val parts = row.split(" ")
      val l = Array.fill(1000)(-1.0)
      l(parts(0).toInt - 1) = 1.0
      (DenseVector(l), DenseVector(parts.tail.map(_.toDouble)))
    }.cache().setName("trainLabelFeatures")

    val trainingFeatures = trainLabelFeatures.map(_._2)
    val trainingLabels = trainLabelFeatures.map(_._1)

    val testLabelFeatures = sc.textFile(conf.testFeaturesDir).map { row =>
      val parts = row.split(" ")
      (Array(parts(0).toInt - 1), DenseVector(parts.tail.map(_.toDouble)))
    }.cache().setName("testLabelFeatures")
    val testFeatures = testLabelFeatures.map(_._2)
    val testActual = testLabelFeatures.map(_._1)

    // val testFeatures = CsvDataLoader(
    //   sc,
    //   conf.testFeaturesDir, 1600, " ").cache().setName("testFeatures")

    // val testActual = CsvDataLoader(
    //   sc,
    //   conf.testActualDir, 1600, " ").map(x => Array(x(0).toInt - 1)).cache().setName("testActual")

    val numTrainImgs = trainingFeatures.count
    trainingLabels.count
    val numTestImgs = testFeatures.count
    testActual.count

    // Lets groupBy class and then unpersist the parent RDDs
    val (groupedFeatures, groupedLabels) = 
        BlockWeightedLeastSquaresEstimator.groupByClasses(Seq(trainingFeatures), trainingLabels)

    groupedFeatures(0).cache()
    groupedFeatures(0).count

    groupedLabels.cache()
    groupedLabels.count

    trainLabelFeatures.unpersist()

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      4096, 1, conf.lambda, conf.mixtureWeight).fit(groupedFeatures, groupedLabels)

    // Apply the model to test data and compute test error
    val testPredictedValues = model(testFeatures)
    val testPredicted = TopKClassifier(5).apply(testPredictedValues)
    logInfo("Top-5 TEST Error is " + Stats.getErrPercent(testPredicted, testActual, numTestImgs) + "%")

    val testPredictedTop1 = TopKClassifier(1).apply(testPredictedValues)
    logInfo("Top-1 TEST Error is " + Stats.getErrPercent(testPredictedTop1, testActual, numTestImgs) + "%")
  }

  case class BlockWeightedSolverConfig(
    trainFeaturesDir: String = "",
    trainLabelsDir: String = "",
    testFeaturesDir: String = "",
    testActualDir: String = "",
    lambda: Double = 6e-5,
    mixtureWeight: Double = 0.25)

  def parse(args: Array[String]): BlockWeightedSolverConfig = {
    new OptionParser[BlockWeightedSolverConfig](appName) {
      head(appName, "0.1")
      help("help") text("prints this usage text")
      opt[String]("trainFeaturesDir") required() action { (x,c) => c.copy(trainFeaturesDir=x) }
      opt[String]("trainLabelsDir") required() action { (x,c) => c.copy(trainLabelsDir=x) }
      opt[String]("testFeaturesDir") required() action { (x,c) => c.copy(testFeaturesDir=x) }
      opt[String]("testActualDir") required() action { (x,c) => c.copy(testActualDir=x) }

      // Solver params
      opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
      opt[Double]("mixtureWeight") action { (x,c) => c.copy(mixtureWeight=x) }
    }.parse(args, BlockWeightedSolverConfig()).get
  }

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
