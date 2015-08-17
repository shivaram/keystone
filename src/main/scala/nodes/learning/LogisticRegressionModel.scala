package nodes.learning

import breeze.linalg.{DenseMatrix, DenseVector, Vector}
import org.apache.spark.mllib.classification.{LogisticRegressionModel => MLlibLRM, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.MLlibUtils.breezeVectorToMLlib
import workflow.{WeightedNode, Transformer, LabelEstimator}

import scala.reflect.ClassTag

class LogisticRegressionModel[T <: Vector[Double]](val model: MLlibLRM) extends Transformer[T, Double] {

  /**
   * Transforms a feature vector to a vector containing the log(posterior probabilities) of the different classes
   * according to this naive bayes model.

   * @param in The input feature vector
   * @return Log-posterior probabilites of the classes for the input features
   */
  override def apply(in: T): Double = {
    model.predict(breezeVectorToMLlib(in))
  }
}


case class LogisticRegressionLBFGSEstimator[T <: Vector[Double] : ClassTag](numClasses: Int = 2, numIters: Int = 100, convergenceTol: Double = 1E-4, cache: Boolean = false)
  extends LabelEstimator[T, Double, Int] with WeightedNode {
  def weight = numIters

  override def fit(in: RDD[T], labels: RDD[Int]): LogisticRegressionModel[T] = {
    val labeledPoints = labels.zip(in).map(x => LabeledPoint(x._1, breezeVectorToMLlib(x._2)))
    val trainer = new LogisticRegressionWithLBFGS().setNumClasses(numClasses)
    trainer.setValidateData(false).optimizer.setConvergenceTol(convergenceTol).setNumIterations(numIters)
    val model = if (cache) {
      trainer.run(labeledPoints.cache())
    } else {
      trainer.run(labeledPoints)
    }

    new LogisticRegressionModel(model)
  }
}
