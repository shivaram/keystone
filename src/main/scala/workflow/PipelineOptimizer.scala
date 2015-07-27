package workflow

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import nodes.util.Cacher
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SparkUtilWrapper
import pipelines.Logging

import scala.annotation.tailrec
import scala.collection.mutable

import argonaut._
import Argonaut._


object PipelineRuntimeEstimator extends Logging {

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

  def getChildren(x: Pipeline[_,_], node: Int): Seq[Int] = {
    x.dataDeps.zipWithIndex.filter { case (l,n) => l.contains(node)}.map(_._2) ++
      x.fitDeps.zipWithIndex.filter { case (l,n) => l.contains(node)}.map(_._2)
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
        logInfo(res.toDebugString)
        val memSize = sample.context.getRDDStorageInfo.filter(_.id == res.id).map(_.memSize).head
        res.unpersist()

        Profile(duration, 0L, memSize)
      }
      case estimator: EstimatorNode => {
        val start = System.nanoTime()
        val res = estimator.fit(dataDepRDDs)
        val duration = System.nanoTime() - start

        //This is a hack - basically just serializes the fit estimator and counts the bytes.
        val memSize = SparkUtilWrapper.estimateSize(res)

        Profile(duration, 0L, memSize)
      }
      case datanode: DataNode => {
        val res = datanode.rdd.cache()

        val start = System.nanoTime()
        res.count()
        val duration = System.nanoTime() - start

        logInfo(res.toDebugString)

        val memSize = sample.context.getRDDStorageInfo.filter(_.id == res.id).map(_.memSize).head
        res.unpersist()

        Profile(duration, 0L, memSize)
      }
      case _ => throw new RuntimeException("Only transformers and estimators should be cached.")
    }
    est
  }

  def cachedRuntime(
      x: Pipeline[_,_],
      ind: Int,
      cached: Int => Double,
      runs: Int => Int,
      localWork: Int => Double): Double = {
    if(ind == -1)
      0.0
    else {
      val deps = x.dataDeps(ind) ++ x.fitDeps(ind)
      (localWork(ind) + deps.map(i => cachedRuntime(x, i, cached, runs, localWork)).sum) /
        math.pow(runs(ind), cached(ind))
    }
  }

  def getSuccs(x: Pipeline[_,_]): Map[Int,Seq[Int]] = {
    x.nodes.indices.map(i => (i, getChildren(x, i))).toMap
  }

  def getRuns(x: Pipeline[_,_], cache: Set[Int]): Map[Int,Int] = {
    val succ = getSuccs(x)

    def r(i: Int): Int = {
      if (succ(i).isEmpty) {
        1
      }
      else {
        succ(i).map(j => if(cache.contains(j)) 1 else r(j)).sum
      }
    }

    x.nodes.indices.map(i => (i, r(i))).toMap
  }

  def estimateCachedRunTime[A](x: ConcretePipeline[A,_], cached: Set[Int], data: RDD[A], profs: Option[Map[Int,Profile]] = None): Double = {

    //Gather runtime statistics.
    val profiles = profs match {
      case Some(x) => x
      case None => x.nodes.indices.map(i => (i,estimateNode(x, i, data))).toMap
    }

    x.nodes.indices.map(i => (x.nodes(i), profiles(i))).foreach(println)

    val localWork = x.nodes.indices.map(i => profiles(i).ns.toDouble).toArray

    //Move the cached map from a list[nodeid] => map[nodeid,double]
    val cachedMap = x.nodes.indices.map(i => if (cached contains i) 1.0 else 0.0).toArray

    //Given a pipeline, compute the number of paths from each node to sink.
    val runs = getRuns(x, cached)

    cachedRuntime(x, x.sink, cachedMap, runs, localWork)
  }

  def estimateNodes[A](x: ConcretePipeline[A,_], data: RDD[A]): Map[Int,Profile] = {
    //TODO: Make this a recursive thing that is smart about caching.
    //Essentially estimateNode(pipe, id, profiles: Map[Id,Profile], intermediate_res: Map[Id,Result]
    //Then we proceed recursively, iteratively building up what we need to.
    //for p in parents
    //if p not in intermediate_res
    // (intermediate_res, profiles) = estimateNode(pipe, p, profiles, intermediate_res)
    //Now that we have intermediate res, calculate current node given these.
    //Need to look at executor to figure this out.

    //Alternatively - topologically sort the dag and execute in order, then you won't have anything missing.

    x.nodes.indices.map(i => (i, estimateNode(x, i, data))).toMap
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
    pipe.nodes(nodeToCache) match {
      case e: EstimatorNode => {
        pipe
      }
      case _ => {
        val newNodes = pipe.nodes :+ new Cacher
        var newId = pipe.nodes.length

        val nodeChildren = PipelineRuntimeEstimator.getChildren(pipe, nodeToCache)

        //The new node depends on only the node to cache.
        var newDataDeps = pipe.dataDeps :+ Seq(nodeToCache)
        var newFitDeps = pipe.fitDeps :+ Seq()

        var dataDeps = pipe.dataDeps
        var fitDeps = pipe.fitDeps

        //For each of the nodes children, modify its parents to point to the new id.
        for(p <- nodeChildren) {
          dataDeps = dataDeps.updated(p, replaceParent(pipe.dataDeps(p), nodeToCache, newId))
          fitDeps = fitDeps.updated(p, replaceParent(pipe.fitDeps(p), nodeToCache, newId))
        }

        val newSink = if (nodeToCache == pipe.sink) newId else pipe.sink

        new ConcretePipeline[A,B](newNodes, newDataDeps, newFitDeps, newSink)
      }
    }
  }

  /**
   * Given a pipeline and a list of nodes to cache, actually construct a cached DAG.
   */
  def makeCachedPipeline[A,B](pipe: Pipeline[A,B], cached: Set[Int]): Pipeline[A,B] = {
    //Find the indexes of the new caching nodes.
    val filteredCaches = pipe.nodes.zipWithIndex.filter ( _._1 match {
      case x: EstimatorNode => false
      case _ => true
    }).map(_._2).toSet

    logInfo(s"Filtered caches: ${filteredCaches}")
    val toCache = cached.intersect(filteredCaches)
    logInfo(s"Caching: ${toCache.map(i => (i, pipe.nodes(i)))}")

    var pipeline = pipe
    toCache.foreach { i =>
      logInfo(s"Adding cache: $i, ${pipeline.nodes(i)}")
      pipeline = addCached(pipeline, i)
      logInfo(s"Cached: $i")
      logInfo(s"Dot ${pipeline.toDOTString}")
    }

    pipeline
  }

  def bruteForceOptimizer[A,B](pipe: Pipeline[A,B]): Pipeline[A,B] = ??? /*{
    val (cacheSpec, time) = caches(pipe.nodes.length)  //Todo - filter out cache specs involving an estimator.
      .map(c => (c, PipelineRuntimeEstimator.estimateRunTime(pipe, Some(c)))) //Todo - filter out cache specs where mem usage is too high.
      .minBy(_._2)

    makeCachedPipeline(pipe, cacheSpec)
  }*/

  def caches(len: Int): Iterator[Set[Int]] = ???/*{
    //Todo: make this faster with bit flipping an Array of size `len` and no copies.
    //The loop:
    //   def f(x: Int) { var i = 0L ; while (i < 1L << x) { i+=1 } }
    //is several orders of magnitude faster.
    new Iterator[Seq[Int]] {
      var i = 0L

      def hasNext = {
        i < (1L << len)
      }

      def next(): Seq[Int] = {
        val out = i.toBinaryString.map(_.asDigit)
        i+=1

        (Seq.fill(len - out.length)(0) ++ out).toSet //TODO: This is totally broken!!!
      }
    }
  }*/
}

object GreedyOptimizer extends Logging {
  def cacheMem(caches: Set[Int], profiles: Map[Int, Profile]): Long = {
    caches.map(i => profiles(i).mem).sum
  }

  def greedyCacheSelect[A,B](pipe: Pipeline[A,B], profiles: Map[Int,Profile], maxMem: Long): Set[Int] = {
    //Initial decision is to just cache everything.
    val initSet = pipe.nodes.zipWithIndex.filter(_._1 match {
      case x: EstimatorNode => true
      case _ => false
    }).map(_._2).toSet

    var caches = initSet

    var runs = PipelineRuntimeEstimator.getRuns(pipe, caches)
    var usedMem = cacheMem(caches, profiles)

    def stillRoom(caches: Set[Int], runs: Map[Int, Int], spaceLeft: Long): Boolean = {
      runs.filter(i => i._2 > 1 && !caches.contains(i._1) && profiles(i._1).mem < spaceLeft).nonEmpty
    }

    def selectNext(caches: Set[Int], runs: Map[Int, Int], spaceLeft: Long): Int = {
      val localWork = pipe.nodes.indices.map(i => (i, profiles(i).ns.toDouble)).toMap
      val cacheMap = pipe.nodes.indices.map(i => (i, if (caches.contains(i)) 1.0 else 0.0)).toMap

      log.info(s"CacheMap: ${cacheMap}")

      //Get the uncached node which fits that maximizes savings in runtime.
      pipe.nodes.indices
        .filter(i => cacheMap(i) < 1 && profiles(i).mem < spaceLeft)
        .map(i => ((runs(i)-1)*PipelineRuntimeEstimator.cachedRuntime(pipe, i, cacheMap, runs, localWork), i))
        .maxBy(_._1)._2
    }

    logInfo("Here we go.")
    logInfo(s"Usedmem: ${usedMem}, MaxMem: ${maxMem}, Runs: ${runs}")
    while (usedMem < maxMem && stillRoom(caches, runs, maxMem - usedMem)) {
      logInfo("In the loop.")
      caches = caches + selectNext(caches, runs, maxMem - usedMem)
      runs = PipelineRuntimeEstimator.getRuns(pipe, caches)
      usedMem = cacheMem(caches, profiles)
    }

    //Return the cache set.
    val cacheInfo = caches.map(i => (pipe.nodes(i), profiles(i)))
    logInfo(s"Cached stuff: $cacheInfo")

    caches
  }

  def greedyOptimizer[A,B](pipe: Pipeline[A,B], data: RDD[A], maxMem: Long, profs: Option[Map[Int,Profile]]=None): (Pipeline[A,B], Set[Int]) = {
    //Step 1 - build profile of the uncached execution.
    val profiles = profs match {
      case Some(x) => x
      case None => PipelineRuntimeEstimator.estimateNodes(pipe.asInstanceOf[ConcretePipeline[A,B]], data)
    }

    //Step 2 - do the optimization routine.
    val caches = greedyCacheSelect(pipe, profiles, maxMem)

    println(s"Caches: ${caches}")

    //Step 3 - given the output of the optimization routine, return an updated pipeline.
    (PipelineOptimizer.makeCachedPipeline(pipe, caches), caches)
  }
}


case class Profile(ns: Long, bytes: Long, mem: Long) {
  def +(p: Profile) = Profile(this.ns + p.ns, this.bytes + p.bytes, this.mem + p.mem)
}

trait HasProfile {
  def profile(size: Int): Profile
}


object DAGWriter {

  case class DAG(vertices: Map[String,Profile], edges: List[(String,String)], sink: String)
  implicit def ProfileCodecJson = casecodec3(Profile.apply, Profile.unapply)("loc","bytes","mem")
  implicit def DAGCodecJson = casecodec3(DAG.apply, DAG.unapply)("vertices","edges","sink")

  def toDAG[A,B](pipe: Pipeline[A, B], prof: Map[Int, Profile]): DAG = {
    val p = pipe.asInstanceOf[ConcretePipeline[A,B]]

    //Produce a list of edges from the adjacency list.
    val edges = (p.dataDeps.zipWithIndex.flatMap(m => m._1.map(s => (s,m._2))) ++
                p.fitDeps.zipWithIndex.flatMap(m => m._1.map(s => (s,m._2))))
                .filter(s => !(s._1 == -1 || s._2 == -1))

    val vertices = prof.map(s => (s._1.toString, s._2)).toMap

    DAG(vertices, edges.map(s => (s._1.toString, s._2.toString)).toList, pipe.sink.toString)
  }

  def toJson[A,B](pipe: Pipeline[A,B], prof: Map[Int, Profile]): String = {
    toDAG(pipe, prof).asJson.spaces2
  }

  def toJson(profiles: Map[Int,Profile]): String = {
    profiles.map(x => (x._1.toString,x._2)).asJson.spaces2
  }

  def profilesFromJson(json: String): Map[Int, Profile] = {
    val res = json.decodeOption[Map[String,Profile]]
    res.get.map(x => (x._1.toInt,x._2)).toMap
  }
}
