package nodes.images

import org.scalatest.FunSuite
import pipelines.Logging
import utils.{ChannelMajorArrayVectorizedImage, ImageMetadata, TestUtils, ImageUtils}

class ScalingSuite extends FunSuite with Logging {

  test("Scaling 0.5") {
    val image = TestUtils.loadTestImage("images/000012.jpg")
    val scaledImage = ImageUtils.scaleImage(image, 0.5)
    println(s"Original ${image.metadata.xDim}, ${image.metadata.yDim}")
    println(s"Scaled ${scaledImage.metadata.xDim}, ${scaledImage.metadata.yDim}")
    assert((image.metadata.xDim*0.5).toInt == scaledImage.metadata.xDim, "x dimension must be scaled by 0.5")
    assert((image.metadata.yDim*0.5).toInt == scaledImage.metadata.yDim, "y dimension must be scaled by 0.5")
  }

  test("Scaling 0.25") {
    val image = TestUtils.loadTestImage("images/000012.jpg")
    val scaledImage = ImageUtils.scaleImage(image, 0.25)
    println(s"Original ${image.metadata.xDim}, ${image.metadata.yDim}")
    println(s"Scaled ${scaledImage.metadata.xDim}, ${scaledImage.metadata.yDim}")
    assert((image.metadata.xDim*0.25).toInt == scaledImage.metadata.xDim, "x dimension must be scaled by 0.25")
    assert((image.metadata.yDim*0.25).toInt == scaledImage.metadata.yDim, "y dimension must be scaled by 0.25")
  }
}
