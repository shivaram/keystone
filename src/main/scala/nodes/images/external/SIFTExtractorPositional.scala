package nodes.images.external

import breeze.linalg._
import nodes.images.SIFTExtractorInterface
import org.apache.spark.rdd.RDD
import utils.Image
import utils.external.VLFeat
import pipelines.Transformer

/**
 * Extracts SIFT Descriptors at dense intervals at multiple scales using the vlfeat C library.
 *
 * @param stepSize Spacing between each sampled descriptor.
 * @param binSize Size of histogram bins for SIFT.
 * @param scales Number of scales at which to extract.
 */
class SIFTExtractorPositional (val stepSize: Int = 3, val binSize: Int = 4, val scales: Int = 4, val scaleStep: Int = 1)
  extends Transformer[Image, (DenseMatrix[Float], DenseMatrix[Float])] {
  @transient lazy val extLib = new VLFeat()

  val descriptorSize = 128

  /**
   * Extract SIFTs from an image.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: Image): (DenseMatrix[Float], DenseMatrix[Float]) = {
    val rawDescDataInt = extLib.getSIFTs(in.metadata.xDim, in.metadata.yDim,
      stepSize, binSize, scales, scaleStep, in.getSingleChannelAsFloatArray())
    val (siftDescriptors, siftPositional) = convertRawSift(rawDescDataInt)
    val numCols = siftDescriptors.length/descriptorSize
    val rawDescData = siftDescriptors.map(s => s.toFloat)
    val rawPosData = scalePositions(siftPositional.map(s => s.toFloat), in.metadata.xDim, in.metadata.yDim)

    (new DenseMatrix(descriptorSize, numCols, rawDescData),  new DenseMatrix(3, numCols, rawPosData))
  }


  /**
   *  Scale positions to be in [-0.5, 0.5]
   */

  def scalePositions(pos: Array[Float], width:Int, height:Int):Array[Float] = {
    pos.zipWithIndex.map { x =>
      if ((x._2 + 3) % 3 == 0)  {  // x coord
        (255*x._1/width).toFloat
      } else if ((x._2 + 2) % 3 == 0)  { // y coord
        (255*x._1/height).toFloat
      } else {
        (255*x._1/50.0).toFloat
      }
    }
    }
  /**
   * Convert raw sift + positional data to (Descriptors, PositionalData)
   */

  def convertRawSift(rawSift: Array[Short]): (Array[Short], Array[Short]) = {
    val notPositionalInfo:(((Short, Int))=> Boolean) = { x => ((x._2 + 3) % 131 != 0) && ((x._2 + 2) % 131 != 0) && ((x._2 + 1) % 131 != 0) }
    /* Filter out the last 3 elements of each 131 dimensional descriptor */

    val rawSifts = rawSift.zipWithIndex.filter(notPositionalInfo).map(_._1)
    /* Grab the complement of the above */
   val positions  = rawSift.zipWithIndex.filter(x => !(notPositionalInfo(x))).map(_._1)
    (rawSifts, positions)
  }
}

object SIFTExtractorPositional {
  def apply(stepSize: Int = 3, binSize: Int = 4, scales: Int = 4, scaleStep: Int = 1) = {
    new SIFTExtractorPositional(stepSize, binSize, scales, scaleStep)
  }
}
