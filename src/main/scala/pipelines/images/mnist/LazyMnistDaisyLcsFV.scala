package pipelines.images.mnist

import java.io.File
import scala.reflect.ClassTag

import breeze.linalg._
import breeze.stats._

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import evaluation.MulticlassClassifierEvaluator
import loaders.MnistLoader
import pipelines.Logging

import nodes.images.external.{FisherVector, FisherVectorStub, SIFTExtractor, SIFTExtractorStub}
import nodes.images._
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper, BatchSignedHellingerMapper, MatrixSampler}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}
import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level


object LazyMnistDaisyLcsFV extends Serializable with Logging {
  val appName = "LazyMnistDaisyLcsFV"
  val experiments = Array("Control", "No Fisher Vector", "No Sift", "No Fisher + Sift")
  def makeDoubleArrayCsv[T: ClassTag](filenames: RDD[String], data: RDD[Array[T]]): RDD[String] = {
    filenames.zip(data).map { case (x,y) => x + ","+ y.mkString(",") }
  }

  // Takes in a GMM and splits it into GMMs with a subset of centroids in each model.
  // Number of output GMMs is (number of centroids in gmm) / centroidBatchSize
  def splitGMMCentroids(
      gmm: GaussianMixtureModel,
      centroidBatchSize: Int): Seq[GaussianMixtureModel] = {
    val totalNumCentroids = gmm.means.cols
    val numBatches = math.ceil(totalNumCentroids.toDouble / centroidBatchSize).toInt
    (0 until numBatches).map { batch =>
      val start = batch * centroidBatchSize
      val end = math.min((batch + 1) * centroidBatchSize, totalNumCentroids)
      GaussianMixtureModel(
        gmm.means(::, start until end),
        gmm.variances(::, start until end),
        gmm.weights(start until end) 
      )
    }
  }

  def constructFisherFeaturizer(gmm: GaussianMixtureModel, name: Option[String] = None) = {
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows
    fisherFeaturizer
  }

  def getDaisyFeatures(
      conf: LazyMnistDaisyLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image])
    : (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {

    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val grayRDD = grayscaler(trainParsed)

    val numImgs = trainParsed.count.toInt
    val siftHellinger =
    if (conf.experiment < 2) {
         (new SIFTExtractor(1) then
        BatchSignedHellingerMapper then new MatrixSampler(conf.descDim))
    } else {
        (new SIFTExtractorStub(128, conf.descDim) then
        BatchSignedHellingerMapper)
    }
    val featurizer = siftHellinger
    val pcaTransformedRDD = featurizer(grayRDD)
    val sampler = new ColumnSampler(conf.numGmmSamples)
    val samples: RDD[DenseVector[Double]] = sampler(pcaTransformedRDD).map(convert(_, Double))
    val gmm = new GaussianMixtureModelEstimator(conf.vocabSize).fit(samples)
    val FisherVectorizer =
    if (conf.experiment % 2 == 0) {
        new FisherVector(gmm)
    } else {
        new FisherVectorStub(conf.descDim, conf.vocabSize)
    }
    val fisherFeaturizer =
            FisherVectorizer then
            FloatToDouble then
            MatrixVectorizer then
            NormalizeRows then
            SignedHellingerMapper then
            NormalizeRows

    constructFisherFeaturizer(gmm, Some("sift-fisher"))
    (fisherFeaturizer(pcaTransformedRDD), (grayscaler then featurizer then fisherFeaturizer).apply(testParsed))
  }

  def getMemKb():Int = {
    // Read status file for current process
    val statFileLines = Source.fromFile("/proc/self/status").getLines()
    // Read heap size field
    val vmData = statFileLines.filter(_ contains "VmData").toList.head
    // Convert to int
    vmData.split(" ").filter(_.forall(_.isDigit)).head.toInt
  }

  def run(sc: SparkContext, conf: LazyMnistDaisyLcsFVConfig) {
    val memoryUsage = List[Int]()
    for (i <- (0 until conf.iterations)) {
      val memUsed = getMemKb();
      println(s"ITERATION NUMBER: ${i}, AvailMem ${memUsed *1000} mb")
      memoryUsage :+ memUsed
    // Property of MNIST
    val numClasses = 10

    // Load the data and extract training labels.
    val parsedRDD = MnistLoader(
      sc,
      conf.trainLocation, 10).cache().setName("trainData")

    val trainingLabels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(parsedRDD.map(_.label))

    val testParsedRDD = MnistLoader(
      sc,
      conf.testLocation, 10).cache().setName("testData")

    val testingLabels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(testParsedRDD.map(_.label))

    val trainParsedImgs = (ImageExtractor).apply(parsedRDD)
    val testParsedImgs = (ImageExtractor).apply(testParsedRDD)

    // Get Sift + FV features
    val (trainDaisy, testDaisy) = getDaisyFeatures(conf, trainParsedImgs, testParsedImgs)
    val trainingFeatures = trainDaisy.collect()
    val testFeatures = testDaisy.collect()

    /*
    val numBlocks = math.ceil(conf.vocabSize.toDouble / conf.centroidBatchSize).toInt

    // NOTE(shivaram): one block only contains `centroidBatchSize` worth of Daisy/LCS features
    // (i.e. one of them not both !). So this will 2048 if centroidBatchSize is 16
    val numFeaturesPerBlock = (conf.vocabSize.toDouble * conf.descDim).toInt // 2048 by default
    println("NUM FEATURES PER BLOCK: " + numFeaturesPerBlock);
    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      numFeaturesPerBlock, 1, conf.lambda, conf.mixtureWeight).fit(
        trainingFeatures, trainingLabels, Some(1))
    val testPredictedValues = model.apply(testFeatures)

    val predicted = TopKClassifier(1).apply(testPredictedValues)

    val actual = TopKClassifier(1).apply(testingLabels);

    logInfo("TEST Error is " + Stats.getErrPercent(predicted, actual, actual.count) + "%")
    */
    }

  }

  case class LazyMnistDaisyLcsFVConfig(
    trainLocation: String = "",
    testLocation: String = "",
    lambda: Double = 6e-5,
    mixtureWeight: Double = 0.25,
    descDim: Int = 64,
    vocabSize: Int = 16,
    daisyScaleStep: Int = 1,
    lcsStride: Int = 4,
    lcsBorder: Int = 16,
    lcsPatch: Int = 6,
    experiment: Int = 0,
    iterations: Int = 2,
    numPcaSamples: Int = 100,
    numGmmSamples: Int = 1000);

  def parse(args: Array[String]): LazyMnistDaisyLcsFVConfig = {
    new OptionParser[LazyMnistDaisyLcsFVConfig](appName) {
      head(appName, "0.1")
      help("help") text("prints this usage text")
      opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
      opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
      // Solver params
      opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
      opt[Double]("mixtureWeight") action { (x,c) => c.copy(mixtureWeight=x) }

      // PCA, GMM params
      opt[Int]("descDim") action { (x,c) => c.copy(descDim=x) }
      opt[Int]("vocabSize") action { (x,c) => c.copy(vocabSize=x) }
      opt[Int]("numPcaSamples") action { (x,c) => c.copy(numPcaSamples=x) }
      opt[Int]("numGmmSamples") action { (x,c) => c.copy(numGmmSamples=x) }

      // Daisy, LCS params
      opt[Int]("daisyScaleStep") action { (x,c) => c.copy(daisyScaleStep=x) }
      opt[Int]("lcsStride") action { (x,c) => c.copy(lcsStride=x) }
      opt[Int]("lcsBorder") action { (x,c) => c.copy(lcsBorder=x) }
      opt[Int]("lcsPatch") action { (x,c) => c.copy(lcsPatch=x) }

      opt[Int]("experiment") action { (x,c) => c.copy(experiment=x) }
      opt[Int]("iterations") action { (x,c) => c.copy(iterations=x) }


    }.parse(args, LazyMnistDaisyLcsFVConfig()).get
  }

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName(appName)
    // NOTE: ONLY APPLICABLE IF YOU CAN DONE COPY-DIR
    conf.remove("spark.jars")

    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
