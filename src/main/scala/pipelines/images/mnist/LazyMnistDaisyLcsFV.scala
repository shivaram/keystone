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

import nodes.images.external.{FisherVector, SIFTExtractor}
import nodes.images._
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper, BatchSignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}

import org.apache.log4j.Logger
import org.apache.log4j.Level


object LazyMnistDaisyLcsFV extends Serializable with Logging {
  val appName = "LazyMnistDaisyLcsFV"

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

    val siftHellinger = (new SIFTExtractor(1) then
      BatchSignedHellingerMapper)

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from
    // disk.
    val samplerPCA = new ColumnSampler(conf.numPcaSamples)
    val siftSamples = samplerPCA(siftHellinger(grayRDD)).setName("sift-samples")
    val samps = siftSamples.collect.map(_.toArray)
    println(samps.length)

    val dataMat: DenseMatrix[Float] = DenseMatrix(samps:_*)

       // Part 2: Compute dimensionality-reduced PCA features.
    val featurizer = siftHellinger
    val pcaTransformedRDD = featurizer(grayRDD)

    val sampler = new ColumnSampler(conf.numGmmSamples)
    val samples: RDD[DenseVector[Double]] = sampler(pcaTransformedRDD).map(convert(_, Double))
    /* Write out samples for debugging */
    val collectedSamples = MatrixUtils.shuffleArray(samples.collect()).take(100)
    val gmm = new GaussianMixtureModelEstimator(conf.vocabSize).fit(samples)
    val fisherFeaturizer = constructFisherFeaturizer(gmm, Some("sift-fisher"))

    (fisherFeaturizer(pcaTransformedRDD), (grayscaler then featurizer then fisherFeaturizer).apply(testParsed))
  }


  def run(sc: SparkContext, conf: LazyMnistDaisyLcsFVConfig) {
    var i = 0;
    while (true) {
      i += 1;
      println("ITERATION NUMBER: " + i)
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

    // Get Daisy + FV features
    val (trainDaisy, testDaisy) = getDaisyFeatures(conf, trainParsedImgs, testParsedImgs)
    val trainingFeatures = trainDaisy.collect()
    val testFeatures = testDaisy.collect()
    System.gc()

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
    centroidBatchSize: Int = 16, 
    daisyPcaFile: Option[String] = None,
    daisyGmmMeanFile: Option[String]= None,
    daisyGmmVarFile: Option[String] = None,
    daisyGmmWtsFile: Option[String] = None,
    lcsPcaFile: Option[String] = None,
    lcsGmmMeanFile: Option[String]= None,
    lcsGmmVarFile: Option[String] = None,
    lcsGmmWtsFile: Option[String] = None,
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

      opt[Int]("centroidBatchSize") action { (x,c) => c.copy(centroidBatchSize=x) }

      // Optional file to load stuff from
      opt[String]("daisyPcaFile") action { (x,c) => c.copy(daisyPcaFile=Some(x)) }
      opt[String]("daisyGmmMeanFile") action { (x,c) => c.copy(daisyGmmMeanFile=Some(x)) }
      opt[String]("daisyGmmVarFile") action { (x,c) => c.copy(daisyGmmVarFile=Some(x)) }
      opt[String]("daisyGmmWtsFile") action { (x,c) => c.copy(daisyGmmWtsFile=Some(x)) }

      opt[String]("lcsPcaFile") action { (x,c) => c.copy(lcsPcaFile=Some(x)) }
      opt[String]("lcsGmmMeanFile") action { (x,c) => c.copy(lcsGmmMeanFile=Some(x)) }
      opt[String]("lcsGmmVarFile") action { (x,c) => c.copy(lcsGmmVarFile=Some(x)) }
      opt[String]("lcsGmmWtsFile") action { (x,c) => c.copy(lcsGmmWtsFile=Some(x)) }

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
