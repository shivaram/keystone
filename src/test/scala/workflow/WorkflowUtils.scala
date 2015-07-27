package workflow

import loaders.{LabeledData, VOCLoader}
import nodes.images.external.{GMMFisherVectorEstimator, SIFTExtractor}
import nodes.images.{GrayScaler, MultiLabelExtractor, MultiLabeledImageExtractor, PixelScaler}
import nodes.learning.{NaiveBayesEstimator, BlockLeastSquaresEstimator, ColumnPCAEstimator}
import nodes.nlp.{NGramsFeaturizer, Tokenizer, LowerCase, Trim}
import nodes.stats.{TermFrequency, ColumnSampler, NormalizeRows, SignedHellingerMapper}
import nodes.util._
import org.apache.spark.rdd.RDD
import pipelines.images.voc.VOCSIFTFisher.SIFTFisherConfig
import utils.MultiLabeledImage

object WorkflowUtils {
  def getVocPipeline(trainData: RDD[MultiLabeledImage], conf: SIFTFisherConfig = SIFTFisherConfig()) = {

    val labelGrabber = MultiLabelExtractor andThen
      ClassLabelIndicatorsFromIntArrayLabels(VOCLoader.NUM_CLASSES)

    val trainingLabels = labelGrabber(trainData)
    val trainingData = MultiLabeledImageExtractor(trainData)
    val numTrainingImages = trainingData.count().toInt

    // Part 1: Scale and convert images to grayscale & Extract Sifts.
    val siftExtractor = PixelScaler andThen
      GrayScaler andThen
      new SIFTExtractor(scaleStep = conf.scaleStep)

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from disk.
    // Part 2: Compute dimensionality-reduced PCA features.
    val pcaFeaturizer = {
        val pca = siftExtractor andThen
          ColumnSampler(10000 / numTrainingImages) andThen
          (ColumnPCAEstimator(conf.descDim), trainingData)

        siftExtractor andThen pca.fittedTransformer
    }

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load from disk.
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer = {
      val fisherVector = pcaFeaturizer andThen
        ColumnSampler(10000) andThen
        (GMMFisherVectorEstimator(conf.vocabSize), trainingData)
      pcaFeaturizer andThen fisherVector.fittedTransformer
      } andThen
      FloatToDouble andThen
      MatrixVectorizer andThen
      NormalizeRows andThen
      SignedHellingerMapper andThen
      NormalizeRows

    // Part 4: Fit a linear model to the data.
    val pipeline = fisherFeaturizer andThen
      (new BlockLeastSquaresEstimator(4096, 1, conf.lambda, Some(2 * conf.descDim * conf.vocabSize)),
        trainingData,
        trainingLabels)

    pipeline
  }

  def getNewsgroupsPipeline(trainData: LabeledData[Int, String]) = {
    val pipe = Trim andThen
      LowerCase() andThen
      Tokenizer() andThen
      NGramsFeaturizer(1 to 2) andThen
      TermFrequency(x => 1) andThen
      (CommonSparseFeatures(100), trainData.data) andThen
      (NaiveBayesEstimator(2), trainData.data, trainData.labels) andThen
      MaxClassifier

    pipe
  }
}