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

import nodes.images.external.FisherVector
import nodes.images._
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper, BatchSignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}

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
    : (Iterator[RDD[DenseVector[Float]]], Iterator[RDD[DenseVector[Float]]]) = {

    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val grayRDD = grayscaler(trainParsed)
    val grayRDDTest = grayscaler(testParsed)

    val numImgs = trainParsed.count.toInt
    var daisySamples: Option[RDD[DenseVector[Float]]] = None
    val daisyHellinger = (new DaisyExtractor(4, 2, 2, 2, 2, 4, 8) then
      BatchSignedHellingerMapper)

    val featurizer = daisyHellinger
    val trainingFeatures = List(featurizer(grayRDD).map(_.toDenseVector)).iterator
    val testFeatures = List(featurizer(grayRDDTest).map(_.toDenseVector)).iterator

    (trainingFeatures, testFeatures)
  }

  def getLcsFeatures(
      conf: LazyMnistDaisyLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image])
    : (Iterator[RDD[DenseVector[Double]]], Iterator[RDD[DenseVector[Double]]]) = {

    val numImgs = trainParsed.count.toInt
    var lcsSamples: Option[RDD[DenseVector[Float]]] = None
    // Part 1a: If necessary, perform PCA on samples of the LCS features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.lcsPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
        lcsSamples = Some(
          new ColumnSampler(conf.numPcaSamples, Some(numImgs)).apply(
            pcapipe(trainParsed)).cache().setName("lcs-samples"))
        val pca = new PCAEstimator(conf.descDim).fit(lcsSamples.get)

        new BatchPCATransformer(pca.pcaMat)
      }
    }

    // Part 2: Compute dimensionality-reduced PCA features.
    val featurizer =  new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
      pcaTransformer
    val pcaTransformedRDD = featurizer(trainParsed)

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load
    // from disk.
    val gmm = conf.lcsGmmMeanFile match {
      case Some(f) =>
        new GaussianMixtureModel(
          csvread(new File(conf.lcsGmmMeanFile.get)),
          csvread(new File(conf.lcsGmmVarFile.get)),
          csvread(new File(conf.lcsGmmWtsFile.get)).toDenseVector)
      case None =>
        val samples = lcsSamples.getOrElse { 
          val lcs = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
          new ColumnSampler(conf.numPcaSamples, Some(numImgs)).apply(lcs(trainParsed))
        }
        val vectorPCATransformer = new PCATransformer(pcaTransformer.pcaMat)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(MatrixUtils.shuffleArray(
            vectorPCATransformer(samples).map(convert(_, Double)).collect()).take(1e6.toInt))
    }

    val splitGMMs = splitGMMCentroids(gmm, conf.centroidBatchSize)

    val trainingFeatures = splitGMMs.iterator.map { gmmPart =>
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("lcs-fisher"))
      fisherFeaturizer(pcaTransformedRDD)
    }

    val testFeatures = splitGMMs.iterator.map { gmmPart => 
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("lcs-fisher"))
      (featurizer then fisherFeaturizer).apply(testParsed)
    }

    (trainingFeatures, testFeatures)
  }

  def run(sc: SparkContext, conf: LazyMnistDaisyLcsFVConfig) {

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
    val trainingFeatures = trainDaisy.map(_.map(_.map(_.toDouble)))
    val testFeatures = testDaisy.map(_.map(_.map(_.toDouble)))
    // val trainingFeatures = ZipVectors(Seq(trainDaisy, trainLcs))
    // val testFeatures = ZipVectors(Seq(testDaisy, testLcs))


    // We have one block each of LCS and Daisy for centroidBatchSize
    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      648, 1, conf.lambda, conf.mixtureWeight).fitOnePass(
        trainingFeatures, trainingLabels, 1)

    val testPredictedValues = model.apply(testFeatures)

    val predicted = TopKClassifier(1).apply(testPredictedValues)

    val actual = TopKClassifier(1).apply(testingLabels);

    logInfo("TEST Error is " + Stats.getErrPercent(predicted, actual, actual.count) + "%")
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
    numPcaSamples: Int = 1e7.toInt,
    numGmmSamples: Int = 1e7.toInt)

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

    val conf = new SparkConf().setAppName(appName)
    // NOTE: ONLY APPLICABLE IF YOU CAN DONE COPY-DIR
    conf.remove("spark.jars")

    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
