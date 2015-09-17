package nodes.images
import pipelines.Transformer

import utils.{Image, ImageConversions, ImageUtils}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Scales image for multi scale SIFT/LCS/DAISY
 */

class Scaler(numScales: Int) extends Transformer[Image, Seq[Image]] {
  override def apply(in: Image): Seq[Image] = {
    new Range(0, numScales, 1).map(x => ImageUtils.scaleImage(in, math.pow(math.sqrt(1.0/2.0), x)))
  }
}


