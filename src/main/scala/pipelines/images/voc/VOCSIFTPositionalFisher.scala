package pipelines.images.voc

import java.io.File

import breeze.linalg._
import breeze.stats._
import evaluation.MeanAveragePrecisionEvaluator
import loaders.{VOCDataPath, VOCLabelPath, VOCLoader}
import nodes.images.external.{FisherVector, SIFTExtractorPositional}
import nodes.images.{GrayScaler, MultiLabelExtractor, MultiLabeledImageExtractor, PixelScaler}
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher, ClassLabelIndicatorsFromIntArrayLabels}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import utils.{Image, MatrixUtils}

object VOCSIFTPositionalFisher extends Serializable {
  val appName = "VOCSIFTPositionalFisher"

  def run(sc: SparkContext, conf: SIFTFisherConfig) {

    // Load the data and extract training labels.
    val trainParsed = VOCLoader(
      sc,
      VOCDataPath(conf.trainLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    val labelGrabber = MultiLabelExtractor then
      ClassLabelIndicatorsFromIntArrayLabels(VOCLoader.NUM_CLASSES) then
      new Cacher[DenseVector[Double]]

    val trainingLabels = labelGrabber(trainParsed)

    // Now featurize and apply the model to test data.
    val testParsed = VOCLoader(
      sc,
      VOCDataPath(conf.testLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    // Part 1: Scale and convert images to grayscale.
    val grayscaler = MultiLabeledImageExtractor then PixelScaler then GrayScaler then new Cacher[Image]


    val siftFeaturizer = grayscaler then  new SIFTExtractorPositional(scaleStep = conf.scaleStep)
    val siftTrainFeatures = siftFeaturizer(trainParsed)
    val siftTrainDescriptors = siftTrainFeatures.map(_._1)
    val siftTestFeatures = siftFeaturizer(testParsed)

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from disk.
    val pcaTransformer = conf.pcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val colSampler = new ColumnSampler(conf.numPcaSamples)
        val pca = new PCAEstimator(conf.descDim).fit(colSampler(siftTrainDescriptors))
        new BatchPCATransformer(pca.pcaMat)
      }
    }

    // Part 2: Compute dimensionality-reduced PCA features.
    val pcaTransformedTrainRDD = siftTrainFeatures.map(x => (pcaTransformer(x._1),x._2))
    val pcaTransformedTestRDD = siftTestFeatures.map(x => (pcaTransformer(x._1),x._2))

    val concatenatedTrainRDD = pcaTransformedTrainRDD.map(x => DenseMatrix.vertcat(x._1, x._2))
    val concatenatedTestRDD = pcaTransformedTestRDD.map(x => DenseMatrix.vertcat(x._1, x._2))

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load from disk.
    val sampler = new ColumnSampler(conf.numGmmSamples)
    val gmm = new GaussianMixtureModelEstimator(conf.vocabSize)
              .fit(sampler(concatenatedTrainRDD).map(convert(_, Double)))

    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]]

    val trainingFeatures = fisherFeaturizer(concatenatedTrainRDD)
    val testFeatures = fisherFeaturizer(concatenatedTestRDD)

    // Part 4: Fit a linear model to the data.
    val model = new BlockLeastSquaresEstimator(4096, 1, conf.lambda).fit(
      trainingFeatures, trainingLabels, Some(2 * conf.descDim * conf.vocabSize))

    siftTrainFeatures.unpersist()

    val testActuals = MultiLabelExtractor(testParsed)

    val predictions = model(testFeatures)

    val map = MeanAveragePrecisionEvaluator(testActuals, predictions, VOCLoader.NUM_CLASSES)
    println(s"TEST APs are: ${map.toArray.mkString(",")}")
    println(s"TEST MAP is: ${mean(map)}")
  }

  case class SIFTFisherConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    numParts: Int = 496,
    lambda: Double = 0.5,
    descDim: Int = 80,
    vocabSize: Int = 256,
    scaleStep: Int = 0,
    pcaFile: Option[String] = None,
    gmmMeanFile: Option[String]= None,
    gmmVarFile: Option[String] = None,
    gmmWtsFile: Option[String] = None,
    numPcaSamples: Int = 1e6.toInt,
    numGmmSamples: Int = 1e6.toInt)

  def parse(args: Array[String]): SIFTFisherConfig = new OptionParser[SIFTFisherConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[String]("labelPath") required() action { (x,c) => c.copy(labelPath=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
    opt[Int]("descDim") action { (x,c) => c.copy(descDim=x) }
    opt[Int]("vocabSize") action { (x,c) => c.copy(vocabSize=x) }
    opt[Int]("scaleStep") action { (x,c) => c.copy(scaleStep=x) }
    opt[String]("pcaFile") action { (x,c) => c.copy(pcaFile=Some(x)) }
    opt[String]("gmmMeanFile") action { (x,c) => c.copy(gmmMeanFile=Some(x)) }
    opt[String]("gmmVarFile") action { (x,c) => c.copy(gmmVarFile=Some(x)) }
    opt[String]("gmmWtsFile") action { (x,c) => c.copy(gmmWtsFile=Some(x)) }
    opt[Int]("numPcaSamples") action { (x,c) => c.copy(numPcaSamples=x) }
    opt[Int]("numGmmSamples") action { (x,c) => c.copy(numGmmSamples=x) }
  }.parse(args, SIFTFisherConfig()).get

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
