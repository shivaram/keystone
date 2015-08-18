package workflow

import java.nio.file.{Paths, Files}

import breeze.linalg.DenseVector
import loaders._
import nodes.learning.NaiveBayesEstimator
import nodes.nlp.{LowerCase, Trim, Tokenizer, NGramsFeaturizer}
import nodes.stats.TermFrequency
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.images.imagenet.ImageNetSiftLcsFV.ImageNetSiftLcsFVConfig
import pipelines.speech.TimitPipeline.TimitConfig
import pipelines.{LocalSparkContext, Logging}
import nodes.util.{MaxClassifier, CommonSparseFeatures, Identity}
import utils.{ObjectUtils, Image, TestUtils}
import scala.tools.nsc.io
import sys.process._


class PipelineRewriterSuite extends FunSuite with LocalSparkContext with Logging {

  def makeCacheSpec(pipe: Pipeline[_,_], nodes: Seq[Int]): Array[Int] = {
    var spec = Array.fill(pipe.nodes.length)(0)
    nodes.foreach(n => spec(n) = 1)
    spec
  }

  def makeConcrete[A](pipe: Pipeline[A,_]): ConcretePipeline[A,_] = {
    new ConcretePipeline(pipe.nodes, pipe.dataDeps, pipe.fitDeps, pipe.sink)
  }

  /*def getPredictorPipeline(sc: SparkContext) = {
    val data = sc.parallelize(Array("this is", "some there", "some text that"))
    val labels = sc.parallelize(Array(0, 1, 1))

    val pipe = WorkflowUtils.getNewsgroupsPipeline(LabeledData(labels.zip(data)))

    (pipe, data)
  }*/

  def makePdf(pipe: Pipeline[_,_], outfile: String) = {
    io.File(s"$outfile.dot").writeAll(pipe.toDOTString)
    s"dot -Tpdf -o${outfile}.pdf $outfile.dot" !
  }

  test("Getting and fitting the VOC pipe") {
    sc = new SparkContext("local", "test")

    val vocSamplePath = VOCDataPath(
      TestUtils.getTestResourceFileName("images/vocdata.tar"),
      "VOCdevkit/VOC2007/JPEGImages/", Some(1)
    )
    val vocSampleLabelPath = VOCLabelPath(TestUtils.getTestResourceFileName("images/voclabels.csv"))

    val prop = 0.05
    val data = VOCLoader(sc, vocSamplePath, vocSampleLabelPath).sample(false, prop, 42).repartition(2).cache()
    logInfo(s"Data is size ${data.count}")

    val pipe = WorkflowUtils.getVocPipeline(data)

    makePdf(pipe, "basicVoc")
    val fitPipe = Optimizer.execute(pipe)
    val cFitPipe = makeConcrete(fitPipe)
    makePdf(cFitPipe, "commonSubVoc")
  }

  /*test("Adding caching to pipeline.") {
    sc = new SparkContext("local", "test")

    val (predictorPipeline, data) = getPredictorPipeline(sc)

    val toCache = predictorPipeline.nodes.zipWithIndex.filter { _._1 match {
      case e: EstimatorNode => false
      case _ => true
    }}.map(_._2).toSet

    log.info(s"old debug string: ${predictorPipeline.toDOTString}")
    makePdf(predictorPipeline, "oldPipe")

    val newPipe = PipelineOptimizer.makeCachedPipeline(predictorPipeline, toCache)

    log.info(s"new debug string: ${newPipe.toDOTString}")
    makePdf(newPipe, "newPipe")
  }*/

  /*test("Estimating a transformer") {
    sc = new SparkContext("local", "test")

    val (predictorPipeline, data) = getPredictorPipeline(sc)

    val NGramIndex = 2
    val est = PipelineRuntimeEstimator.estimateNode(
      predictorPipeline.asInstanceOf[ConcretePipeline[String,Int]],
      NGramIndex,
      data)

    log.info(s"Estimate: $est")

    val ests = (0 until predictorPipeline.nodes.length).map(id => {
      PipelineRuntimeEstimator.estimateNode(
        predictorPipeline.asInstanceOf[ConcretePipeline[String,Int]],
        id,
        data
      )
    })

    ests.map(println)
    println(s"Total: ${ests.reduce(_ + _)}")
  }

  test("Counting paths from a => b") {
    val graph = Seq((1,2),(1,3),(2,4),(3,4))

    val res = PipelineRuntimeEstimator.tsort(graph).toSeq
    val sortedEdges = graph.sortBy(i => res.indexOf(i._1)).reverse

    log.info(s"Sorted edges: ${sortedEdges}")

    val counts = PipelineRuntimeEstimator.countPaths(sortedEdges, 4)

    log.info(s"Counts: $counts")

    assert(counts(4) == 1)
    assert(counts(1) == 2)

  }

  test("Convert a DAG to set(edges) and do the counts.") {
    sc = new SparkContext("local", "test")

    val (predictorPipeline, data) = getPredictorPipeline(sc)

    val fitPipeline = Optimizer.execute(predictorPipeline)

    makePdf(fitPipeline, "fitPipe")

    val counts = PipelineRuntimeEstimator.countPaths(fitPipeline)

    log.info(s"Counts: $counts")
  }

  test("Estimate uncached cost of a DAG") {
    sc = new SparkContext("local", "test")

    val (pipe, data) = getPredictorPipeline(sc)
    val fitPipe = Optimizer.execute(pipe)

    val dataFull = loaders.NewsgroupsDataLoader(sc, "/Users/sparks/datasets/20news-bydate-train/")
    val dataSample = dataFull.data.sample(true, 0.01, 42).cache()
    logInfo(s"Data sample size: ${dataSample.count}")

    val ests = PipelineRuntimeEstimator.estimateCachedRunTime(fitPipe.asInstanceOf[ConcretePipeline[String,_]], Set(), dataSample)
    log.info(s"Est: ${ests}")

    val cests = PipelineRuntimeEstimator.estimateCachedRunTime(fitPipe.asInstanceOf[ConcretePipeline[String,_]], Set(1,5,14,6,15,13), dataSample)
    log.info(s"Est: ${cests}")
  }

  test("Make sure we can serialize a thing to JSON") {
    sc = new SparkContext("local", "test")

    val (pipe, data) = getPredictorPipeline(sc)
    val fitPipe = Optimizer.execute(pipe)

    val profiles = PipelineRuntimeEstimator.estimateNodes(makeConcrete(fitPipe), data)

    log.info(s"Json: ${DAGWriter.toJson(fitPipe, profiles)}")
  }

  test("Brute force optimizer on the Newsgorups pipeline") {
    sc = new SparkContext("local", "test")

    val (pipe, data) = getPredictorPipeline(sc)
    val fitPipe = Optimizer.execute(pipe)

    val optimizedPipe = GreedyOptimizer.greedyOptimizer(fitPipe, data, 10000)
  }*/

   /*
  test("Outputting and trying to brute force VOC pipeline") {
    sc = new SparkContext("local", "test")

    val vocSamplePath = VOCDataPath(
      TestUtils.getTestResourceFileName("images/vocdata.tar"),
      "VOCdevkit/VOC2007/JPEGImages/", Some(1)
    )
    val vocSampleLabelPath = VOCLabelPath(TestUtils.getTestResourceFileName("images/voclabels.csv"))

    val prop = 0.05
    val data = VOCLoader(sc, vocSamplePath, vocSampleLabelPath).sample(false, prop, 42).repartition(2).cache()
    logInfo(s"Data is size ${data.count}")

    val pipe = WorkflowUtils.getVocPipeline(data)

    log.info(s"DOT String: ${pipe.toDOTString}")
    val fitPipe = Optimizer.execute(pipe)

    makePdf(fitPipe, "vocPipe")

    val cFitPipe = makeConcrete(fitPipe)
    log.info(s"Concrete DOT String: ${cFitPipe.toDOTString}")

    val estNodes = cFitPipe.nodes.zipWithIndex.filter { _._1 match {
      case e: EstimatorNode => true
      case _ => false
    }}.map(_._2).toSet

    logInfo(s"Estimator nodes: $estNodes")

    val profilesFilename = s"vocprofiles$prop.json"

    val profiles = if(Files.exists(Paths.get(profilesFilename))) {
      DAGWriter.profilesFromJson(ObjectUtils.readFile(profilesFilename))
    } else {
      val profs = PipelineRuntimeEstimator.estimateNodes(cFitPipe, data.map(_.image))
      ObjectUtils.writeFile(DAGWriter.toJson(profs), profilesFilename)
      profs
    }

    log.info(s"VOC JSON String: ${DAGWriter.toJson(fitPipe, profiles)}")

    val uncachedEstimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      estNodes,
      data.map(_.image),
      Some(profiles)
    )
    logInfo(s"Uncached estimated time: ${uncachedEstimatedTime}")

    val (optimizedPipe, caches) = GreedyOptimizer.greedyOptimizer(cFitPipe, data.map(_.image), 1000*1024*1024, Some(profiles))
    logInfo(s"VOC Optimized JSON String: ${optimizedPipe.toDOTString}")
    makePdf(optimizedPipe, "optimizedVocPipe")

    val start = System.nanoTime
    val res = optimizedPipe(data.map(_.image))
    val greedycount = res.count
    val actualTime = System.nanoTime - start


    val estimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      caches.union(estNodes),
      data.map(_.image),
      Some(profiles)
    )
    logInfo(s"Greedy estimated time: ${estimatedTime}")

    def relativeTime(actual: Long, predicted: Double): Double = predicted/actual

    log.info(s"GREEDY Actual time: $actualTime, Estimated time: $estimatedTime")
    log.info(s"GREEDY Relative: ${relativeTime(actualTime,estimatedTime)}")

    val allpipe = PipelineOptimizer.makeCachedPipeline(cFitPipe, cFitPipe.nodes.indices.toSet)

    val allStart = System.nanoTime()
    val allres = allpipe(data.map(_.image))
    val count = allres.count
    val allActualTime = System.nanoTime() - allStart

    val allEstimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      cFitPipe.nodes.indices.toSet,
      data.map(_.image),
      Some(profiles)
    )

    log.info(s"EVERYTHING Actual time: $allActualTime, Estimated time: $allEstimatedTime")
    log.info(s"EVERYTHING Relative: ${relativeTime(allActualTime,allEstimatedTime)}")

  } */

  /*test("Trying levels of memory for VOC and greedy optimizer.") {
    sc = new SparkContext("local", "test")

    val vocSamplePath = VOCDataPath(
      TestUtils.getTestResourceFileName("images/vocdata.tar"),
      "VOCdevkit/VOC2007/JPEGImages/", Some(1)
    )
    val vocSampleLabelPath = VOCLabelPath(TestUtils.getTestResourceFileName("images/voclabels.csv"))

    val prop = 0.05
    val data = VOCLoader(sc, vocSamplePath, vocSampleLabelPath).sample(false, prop, 42).repartition(2).cache()
    logInfo(s"Data is size ${data.count}")

    val pipe = WorkflowUtils.getVocPipeline(data)

    log.info(s"DOT String: ${pipe.toDOTString}")
    val fitPipe = Optimizer.execute(pipe)
    val cFitPipe = makeConcrete(fitPipe)

    val estNodes = cFitPipe.nodes.zipWithIndex.filter { _._1 match {
      case e: EstimatorNode => true
      case _ => false
    }}.map(_._2).toSet

    logInfo(s"Estimator nodes: $estNodes")

    val profilesFilename = s"vocprofiles$prop.json"

    val profiles = DAGWriter.profilesFromJson(ObjectUtils.readFile(profilesFilename))

    val mems = Array(0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000)

    val res = mems.map(m => {
      val (optimizedPipe, caches) = GreedyOptimizer.greedyOptimizer(cFitPipe, data.map(_.image), m * 1024 * 1024, Some(profiles))
      val estimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
        cFitPipe,
        caches.union(estNodes),
        data.map(_.image),
        Some(profiles)
      )
      (m, estimatedTime)
    })

    for ( l <- res) {
      log.info(s"Result: ${l._1}, ${l._2}")
    }

    val res2 = mems.map(m => {
      logInfo(s"Brute forcing for m MB: $m")
      val (optimizedPipe, caches) = PipelineOptimizer.prunedBruteForceOptimizer(cFitPipe, data.map(_.image), estNodes, m * 1024 * 1024, profiles)
      val estimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
        cFitPipe,
        caches.union(estNodes),
        data.map(_.image),
        Some(profiles)
      )
      (m, estimatedTime)
    })

    for ( l <- res2) {
      log.info(s"Brute force result: ${l._1}, ${l._2}")
    }
  }*/

  /*def estimateAndOptimize[A](data: RDD[A], pipe: Pipeline[A,_], basename: String, prop: Double) = {
    log.info(s"DOT String: ${pipe.toDOTString}")
    val fitPipe = Optimizer.execute(pipe)

    val cFitPipe = makeConcrete(fitPipe)

    val estNodes = cFitPipe.nodes.zipWithIndex.filter { _._1 match {
      case e: EstimatorNode => true
      case _ => false
    }}.map(_._2).toSet

    logInfo(s"Estimator nodes: $estNodes")

    val profilesFilename = s"$basename$prop.json"

    val profiles = if(Files.exists(Paths.get(profilesFilename))) {
      DAGWriter.profilesFromJson(ObjectUtils.readFile(profilesFilename))
    } else {
      val profs = PipelineRuntimeEstimator.estimateNodes(cFitPipe, data)
      ObjectUtils.writeFile(DAGWriter.toJson(profs), profilesFilename)
      profs
    }

    log.info(s"VOC JSON String: ${DAGWriter.toJson(fitPipe, profiles)}")

    val uncachedEstimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      estNodes,
      data,
      Some(profiles)
    )
    logInfo(s"Uncached estimated time: ${uncachedEstimatedTime}")

    val (optimizedPipe, caches) = GreedyOptimizer.greedyOptimizer(cFitPipe, data, 1000*1024*1024, Some(profiles))
    logInfo(s"VOC Optimized JSON String: ${optimizedPipe.toDOTString}")
    makePdf(optimizedPipe, s"optimized${basename}Pipe")

    val start = System.nanoTime
    val res = optimizedPipe(data)
    val greedycount = res.count
    val actualTime = System.nanoTime - start


    val estimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      caches.union(estNodes),
      data,
      Some(profiles)
    )
    logInfo(s"Greedy estimated time: ${estimatedTime}")

    def relativeTime(actual: Long, predicted: Double): Double = predicted/actual

    log.info(s"GREEDY Actual time: $actualTime, Estimated time: $estimatedTime")
    log.info(s"GREEDY Relative: ${relativeTime(actualTime,estimatedTime)}")

    val allpipe = PipelineOptimizer.makeCachedPipeline(cFitPipe, cFitPipe.nodes.indices.toSet)

    val allStart = System.nanoTime()
    val allres = allpipe(data)
    val count = allres.count
    val allActualTime = System.nanoTime() - allStart

    val allEstimatedTime = PipelineRuntimeEstimator.estimateCachedRunTime(
      cFitPipe,
      cFitPipe.nodes.indices.toSet,
      data,
      Some(profiles)
    )

    log.info(s"EVERYTHING Actual time: $allActualTime, Estimated time: $allEstimatedTime")
    log.info(s"EVERYTHING Relative: ${relativeTime(allActualTime,allEstimatedTime)}")
  }*/

  /*test("Optimizing ImageNet pipeline.") {
    sc = new SparkContext("local", "test")

    val prop = 5.0/1.2e6
    val imagesFile = TestUtils.getTestResourceFileName("images/imagenet/shrink_n15075141.tar")
    val labelsFile = TestUtils.getTestResourceFileName("images/imagenet-test-labels")
    val data = ImageNetLoader(sc, imagesFile, labelsFile).cache()

    logInfo(s"Data is size ${data.count}")
    val conf = ImageNetSiftLcsFVConfig(numPcaSamples = 1e4.toInt, numGmmSamples = 1e4.toInt)
    val pipe = WorkflowUtils.getImnetPipeline(data, conf)

    estimateAndOptimize(data.map(_.image), pipe, "imnet", prop)
  }*/

  /*test("Optimizing TIMIT pipeline.") {
    sc = new SparkContext("local", "test")

    val trainFile = TestUtils.getTestResourceFileName("speech/timit/timitData200.csv")
    val labelsFile = TestUtils.getTestResourceFileName("speech/timit/timitLabels200.csv")
    val allData = TimitFeaturesDataLoader(sc, trainFile, labelsFile, trainFile, labelsFile, 4)
    val data = allData.train
    val prop = data.data.count/2e6

    logInfo(s"Data is size ${data.data.count}")
    val conf = TimitConfig(numCosines = 4)
    val pipe = WorkflowUtils.getTimitPipeline(data, conf)

    estimateAndOptimize(data.data, pipe, "timit", prop)
  }*/

}