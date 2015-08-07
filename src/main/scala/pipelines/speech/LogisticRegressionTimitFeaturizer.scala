package pipelines.speech

import breeze.stats.distributions.{CauchyDistribution, RandBasis, ThreadLocalRandomGenerator}
import loaders.TimitFeaturesDataLoader
import nodes.stats.{CosineRandomFeatures, StandardScaler}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import scopt.OptionParser
import utils.MLlibUtils._


object LogisticRegressionTimitFeaturizer extends Logging {
  val appName = "LR Timit Featurization"

  case class TimitConfig(
    trainDataLocation: String = "",
    trainLabelsLocation: String = "",
    testDataLocation: String = "",
    testLabelsLocation: String = "",
    testOutLocation: String = "",
    trainOutLocation: String = "",
    numParts: Int = 512,
    numCosines: Int = 50,
    gamma: Double = 0.05555,
    rfType: Distributions.Value = Distributions.Gaussian,
    lambda: Double = 0.0,
    numEpochs: Int = 5,
    checkpointDir: Option[String] = None)

  def run(sc: SparkContext, conf: TimitConfig) {

    // Set the constants
    val seed = 123L
    val random = new java.util.Random(seed)
    val randomSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val numCosineFeatures = 4096
    val numCosineBatches = conf.numCosines
    val colsPerBatch = numCosineFeatures + 1

    logInfo("PIPELINE TIMING: Started Featurizing Training Features")

    // Load the data
    val timitFeaturesData = TimitFeaturesDataLoader(
      sc,
      conf.trainDataLocation,
      conf.trainLabelsLocation,
      conf.testDataLocation,
      conf.testLabelsLocation,
      conf.numParts)

    // Build the pipeline
    val trainDataAndLabels = timitFeaturesData.train.labels.zip(timitFeaturesData.train.data).repartition(conf.numParts).cache()
    val trainData = trainDataAndLabels.map(_._2)
    val trainLabels = trainDataAndLabels.map(_._1)

    val featurizer = if (conf.rfType == Distributions.Cauchy) {
      // TODO: Once https://github.com/scalanlp/breeze/issues/398 is released,
      // use a RandBasis for cauchy
      CosineRandomFeatures(
        TimitFeaturesDataLoader.timitDimension,
        numCosineFeatures * numCosineBatches,
        conf.gamma,
        new CauchyDistribution(0, 1),
        randomSource.uniform)
    } else {
      CosineRandomFeatures(
        TimitFeaturesDataLoader.timitDimension,
        numCosineFeatures * numCosineBatches,
        conf.gamma,
        randomSource.gaussian,
        randomSource.uniform)
    } andThen (new StandardScaler(), trainData)

    val lrTrainingFeatures = featurizer.apply(trainData)
    val lrTrainData = trainLabels.zip(lrTrainingFeatures).map {
      case (label, features) =>
        LabeledPoint(label, breezeVectorToMLlib(features))
    }

    lrTrainData.saveAsObjectFile(conf.trainOutLocation)

    logInfo("PIPELINE TIMING: Finished Featurizing Training Features")

    logInfo("PIPELINE TIMING: Started Featurizing Test Features")

    val lrTestFeatures = featurizer.apply(timitFeaturesData.test.data)
    val lrTestData = timitFeaturesData.test.labels.zip(lrTestFeatures).map {
      case (label, features) =>
        LabeledPoint(label, breezeVectorToMLlib(features))
    }

    lrTestData.saveAsObjectFile(conf.testOutLocation)
    logInfo("PIPELINE TIMING: Finished Featurizing Test Features")
  }

  object Distributions extends Enumeration {
    type Distributions = Value
    val Gaussian, Cauchy = Value
  }

  def parse(args: Array[String]): TimitConfig = new OptionParser[TimitConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainDataLocation") required() action { (x,c) => c.copy(trainDataLocation=x) }
    opt[String]("trainLabelsLocation") required() action { (x,c) => c.copy(trainLabelsLocation=x) }
    opt[String]("testDataLocation") required() action { (x,c) => c.copy(testDataLocation=x) }
    opt[String]("testLabelsLocation") required() action { (x,c) => c.copy(testLabelsLocation=x) }
    opt[String]("trainOutLocation") required() action { (x,c) => c.copy(trainOutLocation=x) }
    opt[String]("testOutLocation") required() action { (x,c) => c.copy(testOutLocation=x) }
    opt[String]("checkpointDir") action { (x,c) => c.copy(checkpointDir=Some(x)) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Int]("numCosines") action { (x,c) => c.copy(numCosines=x) }
    opt[Int]("numEpochs") action { (x,c) => c.copy(numEpochs=x) }
    opt[Double]("gamma") action { (x,c) => c.copy(gamma=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
    opt("rfType")(scopt.Read.reads(Distributions withName _)) action { (x,c) => c.copy(rfType = x)}
  }.parse(args, TimitConfig()).get

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