package nodes.images
import pipelines.Transformer

import utils.{Image, ImageConversions, ImageUtils}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Scales image for multi scale SIFT/LCS/DAISY
 */

case class Scaler(scale: Double) extends Transformer[Image, Image] {
  override def apply(in: Image): Image = {
    ImageUtils.scaleImage(in, scale)
  }
}

