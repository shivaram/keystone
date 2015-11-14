package nodes.images.external

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.{Logging, LocalSparkContext}
import utils.{ImageUtils, TestUtils}

class SIFTExtractorPositionalSuite extends FunSuite with Logging {
  test("Test Sift on a single image RDD, scaleStep=1 and scaleStep=0, 0 should have more descriptors") {
    val testImage = TestUtils.loadTestImage("images/000012.jpg")
    val singleImage = ImageUtils.mapPixels(testImage, _/255.0)
    val grayImage = ImageUtils.toGrayScale(singleImage)

    val se1 = SIFTExtractorPositional(scaleStep = 1)
    val (res1, pos1) = se1(grayImage)

    val se0 = SIFTExtractorPositional(scaleStep = 0)
    val (res0, pos0) = se0(grayImage)

    val expectedFirstSum = 0.0
    val expectedTotalSum = 1.02642608E8
    val firstKeyPointSum = sum(res0(::, 0))
    val fullFeatureSum = sum(res0)

    logInfo(s"Scale 1 shape is: ${res1.rows}x${res1.cols}")
    logInfo(s"Scale 0 shape is: ${res0.rows}x${res0.cols}")

    assert(res1.cols < res0.cols)
    assert(firstKeyPointSum == expectedFirstSum)
    assert(fullFeatureSum == expectedTotalSum)

  }

}
