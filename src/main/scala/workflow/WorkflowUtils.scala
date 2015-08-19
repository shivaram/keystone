package workflow

import java.io.File

import breeze.linalg._
import breeze.stats.distributions.{CauchyDistribution, RandBasis, ThreadLocalRandomGenerator}
import loaders.{ImageNetLoader, LabeledData, TimitFeaturesDataLoader, VOCLoader}
import nodes.images._
import nodes.images.external.{FisherVector, GMMFisherVectorEstimator, SIFTExtractor}
import nodes.learning._
import nodes.nlp.{LowerCase, NGramsFeaturizer, Tokenizer, Trim}
import nodes.stats._
import nodes.util._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import pipelines.images.imagenet.ImageNetSiftLcsFV._
import pipelines.images.voc.VOCSIFTFisher.SIFTFisherConfig
import pipelines.speech.TimitPipeline.{Distributions, TimitConfig}
import utils.{Image, LabeledImage, MultiLabeledImage}

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
          ColumnSampler(conf.numPcaSamples / numTrainingImages) andThen
          (ColumnPCAEstimator(conf.descDim), trainingData)

        siftExtractor andThen pca.fittedTransformer
    }

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load from disk.
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer = {
      val fisherVector = pcaFeaturizer andThen
        ColumnSampler(conf.numGmmSamples / numTrainingImages) andThen
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

  def getAmazonPipeline(trainData: RDD[(Int, String)]) = {
    val pipe = Trim andThen
      LowerCase() andThen
      Tokenizer() andThen
      NGramsFeaturizer(1 to 2) andThen
      TermFrequency(x => 1) andThen
      (CommonSparseFeatures(100000), trainData.map(_._2)) andThen
      (LogisticRegressionLBFGSEstimator(numIters = 20, cache = true), trainData.map(_._2), trainData.map(_._1))

    pipe
  }

  def computePCAandFisherBranch(
                                 prefix: Pipeline[Image, DenseMatrix[Float]],
                                 trainingData: RDD[Image],
                                 pcaFile: Option[String],
                                 gmmMeanFile: Option[String],
                                 gmmVarFile: Option[String],
                                 gmmWtsFile: Option[String],
                                 numColSamplesPerImage: Int,
                                 numPCADesc: Int,
                                 gmmVocabSize: Int): Pipeline[Image, DenseVector[Double]] = {

    val sampledColumns = prefix andThen
      ColumnSampler(numColSamplesPerImage)

    // Part 1a: If necessary, perform PCA on samples of the features, or load a PCA matrix from disk.
    // Part 2: Compute dimensionality-reduced PCA features.
    val pcaTransformer = pcaFile match {
      case Some(fname) =>
        new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None =>
        val pca = sampledColumns andThen
          (ColumnPCAEstimator(numPCADesc), trainingData)

        pca.fittedTransformer
    }

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load from disk.
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherVectorTransformer = gmmMeanFile match {
      case Some(f) =>
        val gmm = new GaussianMixtureModel(
          csvread(new File(gmmMeanFile.get)),
          csvread(new File(gmmVarFile.get)),
          csvread(new File(gmmWtsFile.get)).toDenseVector)
        FisherVector(gmm)
      case None =>
        val fisherVector = sampledColumns andThen
          pcaTransformer andThen
          (GMMFisherVectorEstimator(gmmVocabSize), trainingData)
        fisherVector.fittedTransformer
    }

    prefix andThen
      pcaTransformer andThen
      fisherVectorTransformer andThen
      FloatToDouble andThen
      MatrixVectorizer andThen
      NormalizeRows andThen
      SignedHellingerMapper andThen
      NormalizeRows
  }

  def getImnetPipeline(trainData: RDD[LabeledImage], conf: ImageNetSiftLcsFVConfig) = {
    // Load the data and extract training labels.
    val parsedRDD = trainData

    val labelGrabber = LabelExtractor andThen
      ClassLabelIndicatorsFromIntLabels(ImageNetLoader.NUM_CLASSES)
    val trainingLabels = labelGrabber(parsedRDD)
    val numTrainingImgs = trainingLabels.count()

    val trainParsedImgs = ImageExtractor.apply(parsedRDD)

    // Get SIFT + FV feature branch
    val siftBranchPrefix = PixelScaler andThen
      GrayScaler andThen
      new SIFTExtractor(scaleStep = conf.siftScaleStep) andThen
      BatchSignedHellingerMapper

    val siftBranch = computePCAandFisherBranch(
      siftBranchPrefix,
      trainParsedImgs,
      conf.siftPcaFile,
      conf.siftGmmMeanFile,
      conf.siftGmmVarFile,
      conf.siftGmmWtsFile,
      conf.numPcaSamples / numTrainingImgs.toInt,
      conf.descDim, conf.vocabSize)

    // Get LCS + FV feature branch
    val lcsBranchPrefix = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
    val lcsBranch = computePCAandFisherBranch(
      lcsBranchPrefix,
      trainParsedImgs,
      conf.lcsPcaFile,
      conf.lcsGmmMeanFile,
      conf.lcsGmmVarFile,
      conf.lcsGmmWtsFile,
      conf.numPcaSamples / numTrainingImgs.toInt,
      conf.descDim, conf.vocabSize)

    // Combine the two models, and fit the weighted least squares model to the data
    val pipeline = Pipeline.gather {
      siftBranch :: lcsBranch :: Nil
    } andThen
      VectorCombiner() andThen
      (new BlockWeightedLeastSquaresEstimator(
        4096, 1, conf.lambda, conf.mixtureWeight, Some(2 * 2 * conf.descDim * conf.vocabSize)),
        trainParsedImgs,
        trainingLabels) andThen
      TopKClassifier(5)

    //Return the unoptimized pipeline.
    pipeline
  }

  def getTimitPipeline(timitFeaturesData: RDD[(Int, DenseVector[Double])], conf: TimitConfig) = {

    val seed = 123L
    val random = new java.util.Random(seed)
    val randomSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val numCosineFeatures = 4096
    val numCosineBatches = conf.numCosines
    val colsPerBatch = numCosineFeatures + 1

    val labels = ClassLabelIndicatorsFromIntLabels(TimitFeaturesDataLoader.numClasses).apply(
      timitFeaturesData.map(_._1)
    ).cache().setName("trainLabels")

    // Train the model
    val featurizer = Pipeline.gather {
      Seq.fill(numCosineBatches) {
        if (conf.rfType == Distributions.Cauchy) {
          // TODO: Once https://github.com/scalanlp/breeze/issues/398 is released,
          // use a RandBasis for cauchy
          CosineRandomFeatures(
            TimitFeaturesDataLoader.timitDimension,
            numCosineFeatures,
            conf.gamma,
            new CauchyDistribution(0, 1),
            randomSource.uniform)
        } else {
          CosineRandomFeatures(
            TimitFeaturesDataLoader.timitDimension,
            numCosineFeatures,
            conf.gamma,
            randomSource.gaussian,
            randomSource.uniform)
        }
      }
    } andThen VectorCombiner()

    val predictorPipeline = featurizer andThen
      (new BlockLeastSquaresEstimator(numCosineFeatures, conf.numEpochs, conf.lambda),
        timitFeaturesData.map(_._2), labels) andThen MaxClassifier

    predictorPipeline
  }
}