package nodes.images.external

import breeze.linalg._
import nodes.images.FisherVectorInterface
import nodes.learning.GaussianMixtureModel
import org.apache.spark.rdd.RDD
import utils.external.EncEval

/**
 * Implements a wrapper for the `enceval` Fisher Vector implementation.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
class FisherVectorStub(numDims:Int, numCentroids:Int)
  extends FisherVectorInterface {
  override def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    DenseMatrix.zeros[Float](numDims, numCentroids*2)
  }
}

