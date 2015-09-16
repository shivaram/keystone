package utils

import java.awt.image.BufferedImage
import java.awt.image.AffineTransformOp
import java.awt.geom.AffineTransform
import java.io.InputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File

import javax.imageio.ImageIO


import pipelines.Logging

import com.sksamuel.scrimage

object ImageUtils extends Logging {

  /**
   * Load image from file.
   * @param fileBytes Bytes of an input file.
   * @return
   */
  def loadImage(fileBytes: InputStream): Option[Image] = {
    classOf[ImageIO].synchronized {
      try {
        val img = ImageIO.read(fileBytes)
        if (img != null) {
          if (img.getHeight() < 36 || img.getWidth() < 36) {
            logWarning(s"Ignoring SMALL IMAGE ${img.getHeight}x${img.getWidth()}")
            None
          } else {
            if (img.getType() == BufferedImage.TYPE_3BYTE_BGR) {
              val imgW = ImageConversions.bufferedImageToWrapper(img)
              Some(imgW)
            } else if (img.getType() == BufferedImage.TYPE_BYTE_GRAY) {
              val imgW = ImageConversions.grayScaleImageToWrapper(img)
              Some(imgW)
            } else {
              logWarning(s"Ignoring image, not RGB or Grayscale of type ${img.getType}")
              None
            }
          }
        } else {
          logWarning(s"Failed to parse image, (result was null)")
          None
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to parse image: due to ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Scale image
   * @param in Input image.
   * @param scalePercent perecnt to scale image by
   */
  def scaleImage(in: Image, scale: Double): Image = {
    if (scale == 1.0) {
      in
    } else {
      val buffImage = ImageConversions.imageToBufferedImage(in)
      val x = (in.metadata.xDim * math.sqrt(scale)).toInt
      val y = (in.metadata.yDim * math.sqrt(scale)).toInt
      val scaled =  new BufferedImage(y, x, buffImage.getType())
      val at = AffineTransform.getScaleInstance(math.sqrt(scale), math.sqrt(scale))

      val scaleOp = new AffineTransformOp(at, AffineTransformOp.TYPE_BILINEAR);
      scaleOp.filter(buffImage, scaled)

      val scaledImage = ImageConversions.bufferedImageToWrapper(scaled)

      scaledImage
    }
  }



  /**
   * Converts an input image to Grayscale according to the NTSC standard weights for RGB images and
   * using sqrt sum of squares for images with other numbers of channels.
   *
   * @param in Input image.
   * @return Grayscaled image.
   */
  def toGrayScale(in: Image): Image = {
    // From the Matlab docs for rgb2gray:
    // rgb2gray converts RGB values to grayscale values by forming a weighted sum of the R, G, and B
    // components: 0.2989 * R + 0.5870 * G + 0.1140 * B

    val numChannels = in.metadata.numChannels
    val out = new ChannelMajorArrayVectorizedImage(new Array(in.metadata.xDim * in.metadata.yDim),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, 1))
    var i = 0
    while (i < in.metadata.xDim) {
      var j = 0
      while (j < in.metadata.yDim) {
        var sumSq = 0.0
        var k = 0
        if (numChannels == 3) {
          // Assume data is in BGR order. Todo - we should check the metadata for this.
          val px = 0.2989 * in.get(i, j, 2) + 0.5870 * in.get(i, j, 1) + 0.1140 * in.get(i, j, 0)
          out.put(i, j, 0, px)
        }
        else {
          while (k < numChannels) {
            sumSq = sumSq + (in.get(i, j, k) * in.get(i, j, k))
            k = k + 1
          }
          val px = math.sqrt(sumSq/numChannels)
          out.put(i, j, 0, px)
        }
        j = j + 1
      }
      i = i + 1
    }
    out
  }

  /**
   * Apply a function to every pixel in the image.
   * NOTE: This function creates a copy of the input image and does not affect the input image.
   *
   * @param in image to apply function to
   * @param fun function that maps pixels from input to output
   * @return new image that is the result of applying the function.
   */
  def mapPixels(in: Image, fun: Double => Double): Image = {
    val out = new ChannelMajorArrayVectorizedImage(
      new Array[Double](in.metadata.xDim * in.metadata.yDim * in.metadata.numChannels),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, in.metadata.numChannels))

    var x, y, c = 0
    while (x < in.metadata.xDim) {
      y = 0
      while (y < in.metadata.yDim) {
        c = 0
        while (c < in.metadata.numChannels) {
          out.put(x, y, c, fun(in.get(x,y,c)))
          c+=1
        }
        y+=1
      }
      x+=1
    }
    out
  }

  /**
   * Combine two images applying a function on corresponding pixels.
   * Requires both images to be of the same size
   *
   * @param in First input image
   * @param in2 Second input image
   * @param fun Function that takes in a pair of pixels and returns the pixel in the combined image
   * @return Combined image
   */
  def pixelCombine(in: Image, in2: Image, fun: (Double, Double) => Double = _ + _): Image = {
    require(in.metadata.xDim == in2.metadata.xDim &&
      in.metadata.yDim == in2.metadata.yDim &&
      in.metadata.numChannels == in2.metadata.numChannels,
      "Images must have the same dimension.")

    val out = new ChannelMajorArrayVectorizedImage(
      new Array[Double](in.metadata.xDim * in.metadata.yDim * in.metadata.numChannels),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, in.metadata.numChannels))

    var x, y, c = 0
    while (x < in.metadata.xDim) {
      y = 0
      while (y < in.metadata.yDim) {
        c = 0
        while (c < in.metadata.numChannels) {
          out.put(x, y, c, fun(in.get(x, y, c), in2.get(x, y, c)))
          c += 1
        }
        y += 1
      }
      x += 1
    }
    out
  }


  /**
   * Convolves images with two one-dimensional filters.
   *
   * @param img Image to be convolved.
   * @param xFilter Horizontal convolution filter.
   * @param yFilter Vertical convolution filter.
   * @return Convolved image
   */
  def conv2D(img: Image, xFilter: Array[Double], yFilter: Array[Double]): Image = {
    val paddedXDim = img.metadata.xDim + xFilter.length - 1
    val paddedYDim = img.metadata.yDim + yFilter.length - 1
    val imgPadded = new RowMajorArrayVectorizedImage(new Array[Double](paddedXDim * paddedYDim *
      img.metadata.numChannels), ImageMetadata(paddedXDim, paddedYDim, img.metadata.numChannels))

    val xPadLow = math.floor((xFilter.length - 1).toFloat / 2).toInt
    // Since we go from 0 to paddedXDim
    val xPadHigh = (paddedXDim - 1) - math.ceil((xFilter.length - 1).toFloat / 2).toInt

    val yPadLow = math.floor((yFilter.length - 1).toFloat / 2).toInt
    // Since we go from 0 to paddedYDim
    val yPadHigh = (paddedYDim - 1) - math.ceil((yFilter.length - 1).toFloat / 2).toInt

    var c = 0
    while (c < img.metadata.numChannels) {
      var y = 0
      while (y < paddedYDim) {
        var yVal = -1
        if (y < yPadLow || y > yPadHigh) {
          yVal = 0
        }
        var x = 0
        while (x < paddedXDim) {
          var xVal = -1
          if (x < xPadLow || x > xPadHigh) {
            xVal = 0
          }

          var px = 0.0
          if (!(xVal == 0 || yVal == 0)) {
            px = img.get(x - xPadLow, y - yPadLow, c)
          }
          imgPadded.put(x, y, c, px)
          x = x + 1
        }
        y = y + 1
      }
      c = c + 1
    }

    val xFilterToUse = xFilter.reverse
    val yFilterToUse = yFilter.reverse
    val imgChannels = imgPadded.metadata.numChannels
    val imgWidth = imgPadded.metadata.yDim
    val imgHeight = imgPadded.metadata.xDim

    val resWidth = imgWidth - yFilterToUse.length + 1
    val resHeight = imgHeight - xFilterToUse.length + 1

    // Storage area for intermediate output.
    val midres = new ColumnMajorArrayVectorizedImage(
      new Array[Double](resHeight*imgWidth*imgChannels),
      ImageMetadata(resHeight, imgWidth, imgChannels))

    // Storage for final output.
    val res = new ColumnMajorArrayVectorizedImage(
      new Array[Double](resWidth*resHeight*imgChannels),
      ImageMetadata(resHeight, resWidth, imgChannels))

    // First we do the rows.
    var x = 0
    var y, chan, i = 0
    var tmp = 0.0

    while (chan < imgChannels) {
      y = 0
      while (y < imgWidth) {
        x = 0
        while (x < resHeight) {
          i = 0
          tmp = 0.0
          var idxToGet = x + y*paddedXDim + chan*paddedXDim*paddedYDim
          while (i < xFilterToUse.length) {
            tmp += imgPadded.getInVector(idxToGet + i) * xFilterToUse(i)
            i += 1
          }
          midres.put(x, y, chan, tmp)
          x += 1
        }
        y += 1
      }
      chan += 1
    }

    // Then we do the columns.
    x = 0
    y = 0
    chan = 0
    i = 0

    while (chan < imgChannels) {
      x = 0
      while (x < resHeight) {
        y = 0
        while ( y < resWidth) {
          val idxToPut = y + x*resWidth + chan*resWidth*resHeight
          var idxToGet = y + x*imgWidth + chan*imgWidth*resHeight
          i = 0
          tmp = 0.0
          while (i < yFilterToUse.length) {
            tmp += midres.getInVector(idxToGet + i) * yFilterToUse(i)
            i += 1
          }
          res.putInVector(idxToPut, tmp)
          y += 1
        }
        x += 1
      }
      chan += 1
    }
    res
  }

  /**
   * Split an image into a number of images, one per channel of input image.
   *
   * @param in Input image to be split
   * @return Array of images, one per channel of input image
   */
  def splitChannels(in: Image): Array[Image] = {
    val out = new Array[Image](in.metadata.numChannels)
    var c = 0
    while (c < in.metadata.numChannels) {
      val a = ChannelMajorArrayVectorizedImage(
          new Array[Double](in.metadata.xDim * in.metadata.yDim),
          ImageMetadata(in.metadata.xDim, in.metadata.yDim, 1))
      var x = 0
      while (x < in.metadata.xDim) {
        var y = 0
        while (y < in.metadata.yDim) {
          a.put(x, y, 0, in.get(x, y, c))
          y = y + 1
        }
        x = x + 1
      }
      out(c) = a 
      c = c + 1
    }
    out
  }
}
