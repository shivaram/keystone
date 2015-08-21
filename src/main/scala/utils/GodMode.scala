package utils

object GodMode {

  /** * Reflection */

  import scala.language.dynamics

  /**
   * Extends classes with the ability to access protected functions and values.
   * Usage: `object.godMode.protectedFunction(...)`
   */
  implicit class GodMode(a: AnyRef) {
    def god = godMode

    def sudo = godMode

    def godMode = new Object with Dynamic {
      def applyDynamic(fieldName: String)(args: Any*): Long = {

        val method = a.getClass.getDeclaredMethod(fieldName,
          classOf[Int],
          classOf[Boolean],
          classOf[Double],
          classOf[Int],
          classOf[Double],
          classOf[String])
        method.setAccessible(true)
        val preparedArgs = args.map(_.asInstanceOf[Object])

        val start = System.nanoTime()
        method.invoke(a, preparedArgs: _*)
        val end = System.nanoTime()

        end - start
      }
    }
  }

}