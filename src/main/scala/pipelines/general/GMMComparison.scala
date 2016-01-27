package pipelines.general

import scopt.OptionParser
import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import breeze.linalg._
import breeze.numerics._
import breeze.stats._

import nodes.learning._
import pipelines._
import utils._

object GMMComparison extends Logging {
  val appName = "GMMComparison"

  case class GMMComparisonConfig(
    numExamples: Int = 0,
    numCenters: Int = 0,
    inputPath: String = "")

  def run(conf: GMMComparisonConfig) {

    val dataReadBegin = System.nanoTime()

    // Set the constants
    val inputMat = csvread(new File(conf.inputPath))
    assert(conf.numExamples <= inputMat.rows)
    val inputArr = MatrixUtils.shuffleArray(MatrixUtils.matrixToRowArray(inputMat)).take(conf.numExamples)

    val dataReadDone = System.nanoTime()
    logInfo("Data Read Done. Took " + (dataReadDone - dataReadBegin)/1e6 + " ms")

    val nativeBegin = System.nanoTime()
    val nativeGmm = new GaussianMixtureModelEstimator(conf.numCenters).fit(inputArr)

    val nativeGmmDone = System.nanoTime()

    logInfo("GMM Native Done. Took " + (nativeGmmDone - nativeBegin)/1e6 + " ms")

    val begin = System.nanoTime()

    val gmm = new BensGMMEstimator(conf.numCenters).fit(inputArr)

    val gmmDone = System.nanoTime()

    logInfo("GMM Scala Done. Took " + (gmmDone - begin)/1e6 + " ms")
  }

  def parse(args: Array[String]): GMMComparisonConfig = new OptionParser[GMMComparisonConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("inputPath") required() action { (x,c) => c.copy(inputPath=x) }
    opt[Int]("numExamples") required() action { (x,c) => c.copy(numExamples=x) }
    opt[Int]("numCenters") required() action { (x,c) => c.copy(numCenters=x) }
  }.parse(args, GMMComparisonConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)
    run(appConfig)
  }

}
