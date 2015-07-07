package workflow

import nodes.learning.NaiveBayesEstimator
import nodes.nlp.{LowerCase, Trim, Tokenizer, NGramsFeaturizer}
import nodes.stats.TermFrequency
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import nodes.util.{MaxClassifier, CommonSparseFeatures, Identity}
import scala.tools.nsc.io
import sys.process._


class PipelineRewriterSuite extends FunSuite with LocalSparkContext with Logging {

  def makeCacheSpec(pipe: Pipeline[_,_], nodes: Seq[Int]): Array[Int] = {
    var spec = Array.fill(pipe.nodes.length)(0)
    nodes.foreach(n => spec(n) = 1)
    spec
  }

  def getPredictorPipeline(sc: SparkContext) = {
    val data = sc.parallelize(Array("this is", "some there", "some text that"))
    val labels = sc.parallelize(Array(0, 1, 1))

    val pipe = Trim andThen
      LowerCase() andThen
      Tokenizer() andThen
      NGramsFeaturizer(1 to 2) andThen
      TermFrequency(x => 1) andThen
      (CommonSparseFeatures(100), data) andThen
      (NaiveBayesEstimator(2), data, labels) andThen
      MaxClassifier

    (pipe, data)
  }

  def makePdf(pipe: Pipeline[_,_], outfile: String) = {
    io.File(s"$outfile.dot").writeAll(pipe.toDOTString)
    s"dot -Tpdf -o${outfile}.pdf $outfile.dot" !
  }

  test("Adding caching to pipeline.") {
    sc = new SparkContext("local", "test")

    val (predictorPipeline, data) = getPredictorPipeline(sc)

    val toCache = predictorPipeline.nodes.zipWithIndex.filter { _._1 match {
      case e: EstimatorNode => true
      case _ => false
    }}.map(_._2)

    val spec = makeCacheSpec(predictorPipeline, toCache)

    log.info(s"old debug string: ${predictorPipeline.toDOTString}")
    makePdf(predictorPipeline, "oldPipe")

    val newPipe = PipelineOptimizer.makeCachedPipeline(predictorPipeline, spec)

    log.info(s"new debug string: ${newPipe.toDOTString}")
    makePdf(newPipe, "newPipe")
  }

  test("Estimating a transformer") {
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

    val ests = PipelineRuntimeEstimator.estimateCachedRunTime(fitPipe.asInstanceOf[ConcretePipeline[String,_]], Seq(), dataSample)
    log.info(s"Est: ${ests}")

    val cests = PipelineRuntimeEstimator.estimateCachedRunTime(fitPipe.asInstanceOf[ConcretePipeline[String,_]], Seq(1,5,14,6,15,13), dataSample)
    log.info(s"Est: ${cests}")

  }
}