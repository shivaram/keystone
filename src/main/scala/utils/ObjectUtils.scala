package utils

import java.io.{FileInputStream, ObjectInputStream, FileOutputStream, ObjectOutputStream}

import scala.io.Source

object ObjectUtils {

  def saveObject[T](obj: T, filename: String) = {
    val writer = new ObjectOutputStream(new FileOutputStream(filename))
    writer.writeObject(obj)
    writer.close()
  }

  def loadObject[T](filename: String): T = {
    val inp = new ObjectInputStream(new FileInputStream(filename))
    val obj = inp.readObject().asInstanceOf[T]
    inp.close()
    obj
  }

  def writeFile(json: String, filename: String) = {
    scala.tools.nsc.io.File(filename).writeAll(json)
  }

  def readFile(filename: String) = {
    Source.fromFile(filename).mkString
  }
}