package loaders

import java.io.FileInputStream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{ImageMetadata, LabeledImage, ColumnMajorArrayVectorizedImage}
import scala.reflect._


/**
 * Loads images from the MNIST Dataset.
 */
object MnistLoader {
  // We hardcode this because these are properties of the MNIST dataset.
  val nrow = 28
  val ncol = 28
  val nchan = 1
  val numClasses = 10

  val labelSize = 1

  def mnistToBufferedImage(mnist: Array[Double]):ColumnMajorArrayVectorizedImage = {

    val byteLen = nrow*ncol*nchan
    ColumnMajorArrayVectorizedImage(mnist, ImageMetadata(nrow, ncol, nchan))
  }

  def apply(sc: SparkContext, path: String, partitions: Int): RDD[LabeledImage] = {
      val images = 
      CsvDataLoader(sc, path, partitions)
        // The pipeline expects 0-indexed class labels, but the labels in the file are 1-indexed
          .map { x =>
          val img = (mnistToBufferedImage(x(1 until x.length).toArray(classTag[Double])))
          val label = x(0).toInt - 1
          LabeledImage(img, label)
          }
      images.groupBy((x:LabeledImage) => x.label, 10).map(_._2.toList).flatMap(x => x)
  }
}
