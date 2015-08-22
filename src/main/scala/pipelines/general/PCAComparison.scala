package pipelines.general

import breeze.stats.distributions.{CauchyDistribution, RandBasis, ThreadLocalRandomGenerator}
import breeze.linalg.DenseVector
import org.apache.commons.math3.random.MersenneTwister
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.apache.spark.rdd.RDD
import org.netlib.util.intW

import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, BlockCoordinateDescent, RowPartitionedMatrix, TSQR}

import pipelines._

object PCAComparison extends Logging {
  val appName = "PCAComparison"

  case class PCAComparisonConfig(
    local: Boolean = false,
    numRows: Int = 1000000,
    numCols: Int = 128,
    numParts: Int = 512)

  def run(sc: SparkContext, conf: PCAComparisonConfig) {

    Thread.sleep(5000)
    // Set the constants
    val seed = 123L
    val random = new java.util.Random(seed)
    val randomSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    if (conf.local) {
      val begin = System.nanoTime()
      val mat = DenseMatrix.rand(conf.numRows, conf.numCols)
      val dataGen = System.nanoTime()

      // TODO(shivaram): This is just copied from PCA.scala. Refactor this !
      val rows = mat.rows
      val cols = mat.cols

      val s1 = DenseVector.zeros[Double](math.min(mat.rows, mat.cols))
      val v1 = DenseMatrix.zeros[Double](mat.cols, mat.cols)

      // Get optimal workspace size
      // we do this by sending -1 as lwork to the lapack function
      val scratch, work = new Array[Double](1)
      val info = new intW(0)

      lapack.dgesvd("N", "A", rows, cols, scratch, rows, scratch, null, 1, scratch, cols, work, -1, info)

      val lwork1 = work(0).toInt
      val workspace = new Array[Double](lwork1)

      // Perform the SVD with sgesvd
      lapack.dgesvd("N", "A", rows, cols, mat.toArray, rows, s1.data, null, 1, v1.data, cols, workspace, workspace.length, info)

      val pca = v1.t

      val pcaDone = System.nanoTime()

      logInfo("PCA Local Done. Data Gen took " + (dataGen - begin)/1e6 + " ms, PCA took " + (pcaDone - dataGen)/1e6 + " ms")

    } else {
      val begin = System.nanoTime()

      val mat = RowPartitionedMatrix.createRandom(sc, conf.numRows, conf.numCols, conf.numParts, true)
      mat.rdd.count()

      val dataGen = System.nanoTime()

      val rPart = new TSQR().qrR(mat)
      val qrDone = System.nanoTime()

      val svd.SVD(u, s, pca) = svd(rPart)

      val pcaDone = System.nanoTime()
      logInfo("PCA Cluster Done. Data Gen took " + (dataGen - begin)/1e6 +
              " ms, QR took " + (qrDone - dataGen)/1e6 +
              " ms, local SVD took " + (pcaDone - qrDone)/1e6 +
              " ms, total PCA took " + (pcaDone - dataGen)/1e6 + " ms")
    }
  }

  def parse(args: Array[String]): PCAComparisonConfig = new OptionParser[PCAComparisonConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[Int]("numRows") action { (x,c) => c.copy(numRows=x) }
    opt[Int]("numCols") action { (x,c) => c.copy(numCols=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Boolean]("local") action { (x,c) => c.copy(local=x) }
  }.parse(args, PCAComparisonConfig()).get

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
