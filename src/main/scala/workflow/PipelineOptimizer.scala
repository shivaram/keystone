package workflow

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import nodes.util.Cacher
import org.apache.spark.rdd.RDD
import pipelines.Logging

import scala.annotation.tailrec
import scala.collection.mutable

import argonaut._
import Argonaut._


object PipelineRuntimeEstimator {

  //These functions are for topologically sorting a pipeline DAG to efficiently
  //count the number of paths to a sink.
  def tsort[A](edges: Traversable[(A, A)]): Iterable[A] = {
    @tailrec
    def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
      val (noPreds, hasPreds) = toPreds.partition { _._2.isEmpty }
      if (noPreds.isEmpty) {
        if (hasPreds.isEmpty) done else sys.error(hasPreds.toString)
      } else {
        val found = noPreds.map { _._1 }
        tsort(hasPreds.mapValues { _ -- found }, done ++ found)
      }
    }

    val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
      acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
    }
    tsort(toPred, Seq())
  }

  def countPaths[A](edges: Seq[(A,A)], t: A): Map[A,Int] = {
    //Initialize a Map with every count == 1.
    val counts = mutable.Map[A,Int]()

    edges.foreach {
      e => {
        counts(e._1) = 0
        counts(e._2) = 0
      }
    }

    counts(t) = 1

    for ((src,dst) <- edges) {
      counts(src) += counts(dst)
    }

    counts.toMap
  }

  def countPaths(x: Pipeline[_,_]): Map[Int,Int] = {
    val edges = x.dataDeps.zip(x.fitDeps).zipWithIndex.flatMap {
      case ((a,b),i) => (a ++ b).map(n => (n,i))
    }

    val res = PipelineRuntimeEstimator.tsort(edges).toSeq
    val sortedEdges = edges.sortBy(i => res.indexOf(i._1)).reverse

    countPaths(sortedEdges, x.sink)
  }



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

    //Make sure all data dependencies are evaluated
    val dataDeps = pipe.dataDeps(node)
    val dataDepRDDs = dataDeps.map(p => pipe.rddDataEval(p, sample).cache())

    //Force the data to be materialized.
    dataDepRDDs.map(_.count)

    val est = pipe.nodes(node) match {

      case transformer: TransformerNode[_] => {
        ////Now make sure we have all our fit dependencies.
        val fitDeps = pipe.fitDeps(node)
        val fitDepsTransformers = fitDeps.map(pipe.fitEstimator)

        //Fit Dependencies.
        val res = transformer.transformRDD(dataDepRDDs, fitDepsTransformers).cache()

        val start = System.nanoTime()
        res.count()
        val duration = System.nanoTime() - start

        //This is ripped from RDD.toDebugString()
        val memSize = sample.context.getRDDStorageInfo.filter(_.id == res.id).map(_.memSize).head
        res.unpersist()

        Profile(duration, 0L, memSize)
      }
      case estimator: EstimatorNode => {
        val start = System.nanoTime()
        val res = estimator.fit(dataDepRDDs)
        val duration = System.nanoTime() - start

        //This is a hack - basically just serializes the fit estimator and counts the bytes.
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(res)
        oos.close()
        val memSize = baos.size()

        Profile(duration, 0L, memSize)
      }
      case datanode: DataNode => {
        val res = datanode.rdd.cache()

        val start = System.nanoTime()
        res.count()
        val duration = System.nanoTime() - start

        val memSize = sample.context.getRDDStorageInfo.filter(_.id == res.id).map(_.memSize).head
        res.unpersist()

        Profile(duration, 0L, memSize)
      }
      case _ => throw new RuntimeException("Only transformers and estimators should be cached.")
    }
    est
  }

  def estimateCachedRunTime[A](x: ConcretePipeline[A,_], cached: Seq[Int], data: RDD[A]): Double = {
    def cachedRuntime(
        x: Pipeline[_,_],
        ind: Int,
        cached: Int => Double,
        paths: Int => Int,
        localWork: Int => Double): Double = {
      if(ind == -1)
        0.0
      else {
        val deps = x.dataDeps(ind) ++ x.fitDeps(ind)
        (localWork(ind) + deps.map(i => cachedRuntime(x, i, cached, paths, localWork)).sum) /
          math.pow(paths(ind), cached(ind))
      }
    }

    //Gather runtime statistics.
    val profiles = (0 until x.nodes.length).map(i => estimateNode(x, i, data))
    x.nodes.zip(profiles).map(println)

    val localWork = profiles.map(_.ns.toDouble).toArray

    //Move the cached map from a list[nodeid] => map[nodeid,double]
    val cachedMap = (0 until x.nodes.length).map(i => if (cached contains i) 1.0 else 0.0).toArray

    //Given a pipeline, compute the number of paths from each node to sink.
    val pathCounts = countPaths(x)

    cachedRuntime(x, x.sink, cachedMap, pathCounts, localWork)
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
      .map(c => (c, PipelineRuntimeEstimator.estimateRunTime(pipe, Some(c)))) //Todo - filter out cache specs where mem usage is too high.
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

object GreedyOptimizer {
  //def cacheMem()
  //def runs

  def greedyOptimizer[A,B](pipe: Pipeline[A,B]): Pipeline[A,B] = {
    //Step 1 - build profile of the uncached execution.
    //pipe.nodes.map
    pipe
  }
}


case class Profile(ns: Long, bytes: Long, mem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.bytes + p.bytes, this.mem + p.mem)
}

trait HasProfile {
  def profile(size: Int): Profile
}


object DAGWriter {

  case class DAG(vertices: Map[String,Profile], edges: List[(String,String)], sink: Int)
  implicit def ProfileCodecJson = casecodec3(Profile.apply, Profile.unapply)("loc","bytes","mem")
  implicit def DAGCodecJson = casecodec3(DAG.apply, DAG.unapply)("vertices","edges","sink")

  def toDAG[A,B](pipe: Pipeline[A, B], prof: Map[Int, Profile]): DAG = {
    val p = pipe.asInstanceOf[ConcretePipeline[A,B]]

    //Produce a list of edges from the adjacency list.
    val edges = p.dataDeps.zipWithIndex.flatMap(m => m._1.map(s => (s,m._2))) ++
                p.fitDeps.zipWithIndex.flatMap(m => m._1.map(s => (s,m._2)))

    val vertices = prof.map(s => (s._1.toString, s._2)).toMap

    DAG(vertices, edges.map(s => (s._1.toString, s._2.toString)).toList, pipe.sink)
  }

  def toJson[A,B](pipe: Pipeline[A,B], prof: Map[Int, Profile]): String = {
    toDAG(pipe, prof).asJson.spaces2
  }
}

