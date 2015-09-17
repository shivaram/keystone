package nodes.images
import pipelines.Transformer

import breeze.linalg._

import utils.{Image, ImageConversions, ImageUtils}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/* Scales down an image numScales times and then applies a given (Image --> DenseMatrix) Transformation
 * on each image and then cocatenates the resultant matrices */

class ScaledImageTransformer(numScales: Int, t: Transformer[Image, DenseMatrix[Float]]) extends Transformer[Image, DenseMatrix[Float]] {
  override def apply(in: Image): DenseMatrix[Float] = {
    val scaledImages:Seq[Image] = new Scaler(numScales).apply(in)
    val daisyMultiScales = scaledImages
    (new Scaler(numScales).apply(in)).map(image => t.apply(image)).reduce(DenseMatrix.horzcat(_,_))
  }
}


