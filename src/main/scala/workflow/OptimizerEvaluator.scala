package workflow

import java.nio.file.{Paths, Files}

import breeze.linalg.DenseVector
import loaders._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import pipelines.Logging
import pipelines.images.imagenet.ImageNetSiftLcsFV.ImageNetSiftLcsFVConfig
import pipelines.images.voc.VOCSIFTFisher.SIFTFisherConfig
import pipelines.speech.TimitPipeline.TimitConfig
import scopt.OptionParser
import utils.{LabeledImage, MultiLabeledImage, ObjectUtils}

import scala.reflect.ClassTag


object OptimizerEvaluator extends Logging {
  /**
   * Convert a pipeline into a concrete pipeline.
   * @param pipe
   * @tparam A
   * @return
   */
  def makeConcrete[A,_](pipe: Pipeline[A,_]): ConcretePipeline[A,_] = {
    new ConcretePipeline(pipe.nodes, pipe.dataDeps, pipe.fitDeps, pipe.sink)
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }

  /**
   * Load a sample from disk or take a performacne sample of the pipeline at a given scale.
   * @param pipeGetter Function that gets pipeline to sample.
   * @param trainData Data to use to fit the pipeline.
   * @param testData Data to use to estimate end-to-end speed.
   * @param scale Number of elements per partition to sample.
   * @param profilesDir Directory containing previously sampled profiles.
   * @param pipeName Name of this pipeline.
   * @tparam A Pipeline input type.
   */
  def loadOrSample[A : ClassTag, B : ClassTag](
      pipeGetter: RDD[B] => Pipeline[A, _],
      trainData: RDD[B],
      testData: RDD[A],
      scale: Int,
      profilesDir: String,
      pipeName: String): Map[Int, Profile] = {

    //Create directory if it doesn't exist.
    if (Files.notExists(Paths.get(profilesDir))) Files.createDirectory(Paths.get(profilesDir))

    val fileName = s"$profilesDir/pipeName$scale.json"


    val profiles = if(Files.exists(Paths.get(fileName))) {
      //If we already have a sample, read it from file.
      DAGWriter.profilesFromJson(ObjectUtils.readFile(fileName))
    } else {
      //Otherwise, sample the data, materialize it, collect a profile, and write it to file.
      val sampledData = trainData.mapPartitions(_.take(scale)).cache()
      sampledData.count()

      val sampledTestData = testData.mapPartitions(_.take(scale)).cache()
      sampledTestData.count()

      val pipe = pipeGetter(sampledData)
      val fitPipe = Optimizer.execute(pipe)
      val cFitPipe = makeConcrete(fitPipe)

      val profs = PipelineRuntimeEstimator.estimateNodes(cFitPipe, sampledTestData)
      ObjectUtils.writeFile(DAGWriter.toJson(profs), fileName)
      profs
    }

    profiles
  }

  def profileOptimizeAndTime[A : ClassTag, B : ClassTag](pipeGetter: RDD[B] => Pipeline[A, _], trainData: RDD[B], testData: RDD[A], config: OptimizerEvaluatorConfig) = {
    //Step 0: Take the loaded pipeline and make it concrete.
    val cFitPipe = makeConcrete(Optimizer.execute(pipeGetter(trainData)))

    //Step 1: Sample or read from disk.
    logInfo("Generating Samples (or reading from disk.")
    val profiles: Map[Int, Map[Int, Profile]] = config.sampleSizes.map(s => {
      (s, loadOrSample(pipeGetter, trainData, testData, s, config.profilesDir, config.pipeline.toString))
    }).toMap

    //Step 2: Generalize from the samples.
    logInfo("Generalizing from the samples.")
    val newProfile = PipelineRuntimeEstimator.generalizeProfiles(1.0, profiles)

    //Step 3: Optimize.
    logInfo("Optimizing the pipeline.")
    val optimizedPipe = config.cachingStrategy match {
      case CachingStrategy.All => PipelineOptimizer.makeCachedPipeline(cFitPipe, (0 until cFitPipe.nodes.length).toSet)
      case CachingStrategy.EstOnly => cFitPipe
      case CachingStrategy.Greedy => GreedyOptimizer.greedyOptimizer(
        cFitPipe,
        (config.memSizeMb*1024*1024).toLong,
        newProfile)._1
    }

    //Step 4: Run and time.
    logInfo("Beginning the actual pipeline execution.")
    val pipeStart = System.nanoTime()
    val piperes = optimizedPipe(testData)
    val count = piperes.count()
    val pipeTime = System.nanoTime() - pipeStart
    logInfo("Finished pipeline execution.")
  }

  def run(sc: SparkContext, config: OptimizerEvaluatorConfig) = {
    config.pipeline match {
      case TestPipeline.Amazon => {
        val data = AmazonReviewsDataLoader(sc, config.trainLocation, 3.5)
        val pipeGetter = WorkflowUtils.getNewsgroupsPipeline(_ : RDD[(Int, String)])

        profileOptimizeAndTime(pipeGetter, data.labeledData, data.data, config)
      }

      case TestPipeline.TIMIT => {
        val data = TimitFeaturesDataLoader(
          sc,
          config.trainLocation,
          config.trainLabels,
          config.testLocation,
          config.trainLabels,
          config.numPartitions)

        val pipeGetter = WorkflowUtils.getTimitPipeline(_ : RDD[(Int, DenseVector[Double])], TimitConfig())

        profileOptimizeAndTime(pipeGetter, data.train.labeledData, data.test.data, config)
      }

      case TestPipeline.ImageNet => {
        val data = ImageNetLoader(sc, config.trainLocation, config.trainLabels)
        val pipeGetter = WorkflowUtils.getImnetPipeline(_: RDD[LabeledImage], ImageNetSiftLcsFVConfig())

        profileOptimizeAndTime(pipeGetter, data, data.map(_.image), config)
      }

      case TestPipeline.VOC => {
        val data = VOCLoader(
          sc,
          VOCDataPath(config.trainLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(config.numPartitions)),
          VOCLabelPath(config.trainLabels))

        val pipeGetter = WorkflowUtils.getVocPipeline(_ : RDD[MultiLabeledImage], SIFTFisherConfig())

        profileOptimizeAndTime(pipeGetter, data, data.map(_.image), config)
      }

    }

  }

  val appName = "OptimizerEvaluator"

  object TestPipeline extends Enumeration {
    type TestPipeline = Value
    val Amazon, TIMIT, VOC, ImageNet = Value
  }

  object CachingStrategy extends Enumeration {
    type CachingStrategy = Value
    val EstOnly, Greedy, All = Value
  }

  case class OptimizerEvaluatorConfig(
    trainLocation: String = "",
    trainLabels: String = "",
    testLocation: String = "",
    testLabels: String = "",
    profilesDir: String = "profiles",
    memSizeMb: Int = 0,
    sampleSizes: Array[Int] = Array(1, 2),
    numPartitions: Int = 10,
    seed: Long = 0,
    pipeline: TestPipeline.Value = TestPipeline.Amazon,
    cachingStrategy: CachingStrategy.Value = CachingStrategy.Greedy)

  def parse(args: Array[String]): OptimizerEvaluatorConfig = new OptionParser[OptimizerEvaluatorConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("trainLabels") required() action { (x,c) => c.copy(trainLabels=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[String]("testLabels") required() action { (x,c) => c.copy(testLabels=x) }
    opt[String]("profilesDir") action { (x,c) => c.copy(profilesDir=x) }
    opt[String]("memSize") required() action { (x,c) => c.copy(memSizeMb=memoryStringToMb(x)) }
    opt("testPipeline")(scopt.Read.reads(TestPipeline withName _)) required() action { (x,c) => c.copy(pipeline = x)}
    opt("cachingStrategy")(scopt.Read.reads(CachingStrategy withName _)) action { (x,c) => c.copy(cachingStrategy = x)}
    opt[String]("sampleSizes") action { (x,c) => c.copy(sampleSizes=x.split(",").map(_.toInt)) }
    opt[Int]("numPartitions") action { (x,c) => c.copy(numPartitions=x) }
    opt[Long]("seed") action { (x,c) => c.copy(seed=x) }
  }.parse(args, OptimizerEvaluatorConfig()).get

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