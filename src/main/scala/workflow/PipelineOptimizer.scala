package workflow

import nodes.util.Cacher
import org.apache.spark.rdd.RDD
import pipelines.Logging


object PipelineRuntimeEstimator {
  def runtime(node: Int): Double = 2.0

  def getChildren(x: Pipeline[_,_], node: Int): Seq[Int] = {
    x.dataDeps.zipWithIndex.filter { case (l,n) => l.contains(node)}.map(_._2) ++
      x.fitDeps.zipWithIndex.filter { case (l,n) => l.contains(node)}.map(_._2)
  }

  def estimateRunTime(pipe: Pipeline[_,_], node: Int, runtime: Int => Double): Double = {
    val parents = pipe.dataDeps(node) ++ pipe.fitDeps(node)
    runtime(node) + parents.map(p => estimateRunTime(pipe, p, runtime)).reduce(_ + _)
  }

  def estimateRunTime(x: Pipeline[_,_], cached: Option[Seq[Int]]): Double = {
    val runTimeEstimator = cached match {
      case Some(cl) => { x: Int => (1.0 - cl(x))*2.0 }
      case None => runtime _
    }

    estimateRunTime(x, x.sink, runTimeEstimator)
  }

  def estimateNode[A](pipe: ConcretePipeline[A,_], node: Int, sample: RDD[A]): Profile = {
    //TODO: this code only served as an exercise to make sure i could do this stuff.
    //val estimates = pipe.nodes.map( n => {
    //  n match {
    //    case t: HasEstimator => t.estimate(20)
    //    case _ => Estimate(0,0,0)
    //  }
    //}).reduce(_ + _)

    val est = pipe.nodes(node) match {
      case transformer: TransformerNode[_] => {
        //Make sure all data dependencies are evaluated
        val dataDeps = pipe.dataDeps(node)
        val dataDepRDDs = dataDeps.map(p => pipe.rddDataEval(p, sample).cache())

        //Force the data to be materialized.
        dataDepRDDs.map(_.count)

        //Now make sure we have all our data dependencies.
        val fitDeps = pipe.fitDeps(node)
        val fitDepsTransformers = fitDeps.map(pipe.fitEstimator)

        //Fit Dependencies.
        val res = transformer.transformRDD(dataDepRDDs, fitDepsTransformers).cache()

        val start = System.nanoTime()
        res.count()
        val duration = System.nanoTime() - start

        //This is ripped from RDD.toDebugString()
        val memSize = sample.context.getRDDStorageInfo.filter(_.id == res.id).map(_.memSize).head

        Profile(duration, 0L, memSize)
      }
      case _ => throw new RuntimeException("Only transformers should be cached.")
    }
    est
  }
}

object PipelineOptimizer extends Logging {

  /**
   * Given a dependency list, replace any occurrence of some old index with a new one.
   * @param depList
   * @param oldParent
   * @param newParent
   * @return
   */
  def replaceParent(depList: Seq[Int], oldParent: Int, newParent: Int): Seq[Int] = {
    val oldParentIndex = depList.indexOf(oldParent)

    if (oldParentIndex >= 0) depList.updated(oldParentIndex, newParent) else depList
  }

  /**
   * Given a pipeline and an index to cache - return a pipeline with the node cached.
   */
  def addCached[A,B](pipe: Pipeline[A,B], nodeToCache: Int): Pipeline[A, B] = {
    val newNodes = pipe.nodes :+ new Cacher
    var newId = pipe.nodes.length

    val nodeChildren = PipelineRuntimeEstimator.getChildren(pipe, nodeToCache)

    //Set up the data dependencies for the node to cache and the new node.
    val newNodeDataDeps = if (pipe.dataDeps(nodeToCache).length > 0) Seq(nodeToCache) else Seq()
    var newDataDeps = (pipe.dataDeps :+ newNodeDataDeps)

    val newNodeFitDeps = if (pipe.fitDeps(nodeToCache).length > 0) Seq(nodeToCache) else Seq()
    var newFitDeps = (pipe.fitDeps :+ newNodeFitDeps)

    //For each of the nodes children, modify its parents to point to the new id.
    for(p <- nodeChildren) {
      newDataDeps = newDataDeps.updated(p, replaceParent(newDataDeps(p), nodeToCache, newId))
      newFitDeps = newFitDeps.updated(p, replaceParent(newFitDeps(p), nodeToCache, newId))
    }

    val newSink = if (nodeToCache == pipe.sink) newId else pipe.sink

    new ConcretePipeline[A,B](newNodes, newDataDeps, newFitDeps, newSink)
  }

  /**
   * Given a pipeline and a list of nodes to cache, actually construct a cached DAG.
   */
  def makeCachedPipeline[A,B](pipe: Pipeline[A,B], cached: Seq[Int]): Pipeline[A,B] = {
    //Find the indexes of the new caching nodes.
    val cacheIndexes = cached.zipWithIndex.filter(_._1 > 0).map(_._2)

    //Replace the caching node with a new node.
    //Two step process - first, we create a new node.
    var pipeline = pipe
    cacheIndexes.foreach { i =>
      pipeline = addCached(pipeline, i)
    }

    pipeline
  }

  def bruteForceOptimizer[A,B](pipe: Pipeline[A,B]): Pipeline[A,B] = {
    val (cacheSpec, time) = caches(pipe.nodes.length)  //Todo - filter out cache specs involving an estimator.
      .map(c => (c, PipelineRuntimeEstimator
      .estimateRunTime(pipe, Some(c))))
      .minBy(_._2)

    makeCachedPipeline(pipe, cacheSpec)
  }

  def caches(len: Int): Iterator[Seq[Int]] = {
    //Todo: make this faster with bit flipping an Array of size `len` and no copies.
    //The loop:
    //   def f(x: Int) { var i = 0L ; while (i < 1L << x) { i+=1 } }
    //is several orders of magnitude faster.
    new Iterator[Seq[Int]] {
      var i = 0L

      def hasNext() = {
        i < (1L << len)
      }

      def next(): Seq[Int] = {
        val out = i.toBinaryString.map(_.asDigit)
        i+=1

        Seq.fill(len - out.length)(0) ++ out
      }
    }
  }
}

case class Profile(ns: Long, bytes: Long, mem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.bytes + p.bytes, this.mem + p.mem)
}

trait HasProfile {
  def profile(size: Int): Profile
}