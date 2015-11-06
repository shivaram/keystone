package nodes.images.external

import breeze.linalg._
import nodes.images.SIFTExtractorInterface
import org.apache.spark.rdd.RDD
import utils.Image
import utils.external.VLFeat

/**
 * Extracts SIFT Descriptors at dense intervals at multiple scales using the vlfeat C library.
 *
 * @param stepSize Spacing between each sampled descriptor.
 * @param binSize Size of histogram bins for SIFT.
 * @param scales Number of scales at which to extract.
 */
class SIFTExtractorStub(val descriptorSize:Int, val numCols:Int)
  extends SIFTExtractorInterface {

  /**
   * Extract SIFTs from an image.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: Image): DenseMatrix[Float] = {
    DenseMatrix.zeros[Float](descriptorSize, numCols)
  }
}

object SIFTExtractorStub {
  def apply(descriptorSize:Int = 128, numCols:Int = 64) = {
    new SIFTExtractor(descriptorSize, numCols)
  }
}
