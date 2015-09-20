package utils.external

class VLFeat extends Serializable {
  System.loadLibrary("ImageFeatures") // This will load libImageEncoders.{so,dylib} from the library path.

  /**
   * Gets SIFT Descriptors at Multiple Scales emulating the `vl_phow` MATLAB routine.
   * Under the hood it uses vl_dsift from the vlfeat library.
   *
   * @param width Image Width.
   * @param height Image Height.
   * @param step Step size at which to sample SIFT descriptors.
   * @param bin SIFT Descriptor bin size.
   * @param numScales Number of scales to extract at.
   * @param image Input image as float array.
   * @return SIFTs as Ints format is [SiftDesc[128], x, y, s, SiftDesc[128], x, y, s....]
   */
  @native
  def getSIFTs(
      width: Int,
      height: Int,
      step: Int,
      bin: Int,
      numScales: Int,
      scaleStep: Int,
      image: Array[Float]): Array[Int]
}
