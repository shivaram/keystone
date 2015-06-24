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
  }


}