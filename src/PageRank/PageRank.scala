import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object PageRank {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("PageRank"))
    val file = sc.textFile("file:///home/wangyang/Documents/relation.txt")
    val initFile = file.flatMap { _.split("\t").toList }
    //<2, 1> <2, 4> <3, 2>....
    val rankFile = file.map { line =>
      val token = line.split("\t")
      (token(0), token(1))
    }.distinct()
    file.unpersist(true);
    //<2, 1f> <3, 1f>  <4, 1f>...
    var init = initFile.distinct().map { (_, 1f) }
    var map = sc.broadcast(init.collectAsMap())
    //optimal algorithm(0.15f + x._2._1.get * 0.85f)
    val threshold = 1000
    var flag = true
    var diff = 0f
    while(flag) {
      var lastinit = init
      init = rankFile.aggregateByKey(List[String]())(_ :+ _, _ ++ _).flatMap(calculate(_, map.value)).reduceByKey(_ + _)
        .rightOuterJoin(init).map(x => (x._1, if (x._2._1.isDefined == true) x._2._1.get else 0f))
      map = sc.broadcast(init.collectAsMap())

      diff=pr_diff(lastinit.collectAsMap(),init.collectAsMap())

      if (diff <= threshold) {
        flag = false
      }
    }
    val result=init.sortBy(_._2)
    result.collect().foreach(println)
  }

  def calculate(a: (String, List[String]), b: scala.collection.Map[String, Float]): ArrayBuffer[(String, Float)] = {
    val array = ArrayBuffer[(String, Float)]()
    for (i <- 0 to (a._2.size - 1)) {
      array += ((a._2(i), b.get(a._1).get / a._2.size))
    }
    array
  }

  def pr_diff(a: scala.collection.Map[String, Float], b: scala.collection.Map[String, Float]): Float = {
    var nowDiff: Float = 0f

    for ((k,v) <- a){
      if(v > b.get(k).get)
        nowDiff  += (v - b.get(k).get)
      else
        nowDiff += (b.get(k).get - v)
    }
    nowDiff
  }

}
