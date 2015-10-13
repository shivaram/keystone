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
class SIFTExtractor(val stepSize: Int = 3, val binSize: Int = 4, val scales: Int = 4, val scaleStep: Int = 1)
  extends SIFTExtractorInterface {
  @transient lazy val extLib = new VLFeat()

  val descriptorSize = 128
  var positionalData:Array[Short] = Array[Short]()

  /**
   * Extract SIFTs from an image.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: Image): DenseMatrix[Float] = {
    val rawDescDataShort = extLib.getSIFTs(in.metadata.xDim, in.metadata.yDim,
      stepSize, binSize, scales, scaleStep, in.getSingleChannelAsFloatArray())
    val (siftDescriptors, siftPositional) = convertRawSift(rawDescDataShort)
    positionalData = siftPositional
    val numCols = siftDescriptors.length/descriptorSize
    val rawDescData = siftDescriptors.map(s => s.toFloat)
    new DenseMatrix(descriptorSize, numCols, rawDescData)
  }

  /**
   * Convert raw sift + positional data to (Descriptors, PositionalData)
   */

  def convertRawSift(rawSift: Array[Short]): (Array[Short], Array[Short]) = {
    val rawSifts = rawSift.zipWithIndex.filter(x => ((x._2 + 3) % 131 != 0) && ((x._2 + 2) % 131 != 0) && ((x._2 + 1) % 131 != 0)).map(_._1)
    val positions  = rawSift.zipWithIndex.filter(x => !(((x._2 + 3) % 131 != 0) && ((x._2 + 2) % 131 != 0 ) && ((x._2 + 1) % 131 != 0 ))).map(_._1)
    (rawSifts, positions)
  }
}

object SIFTExtractor {
  def apply(stepSize: Int = 3, binSize: Int = 4, scales: Int = 4, scaleStep: Int = 1) = {
    new SIFTExtractor(stepSize, binSize, scales, scaleStep)
  }
}
