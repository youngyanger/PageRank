import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps
import org.apache.spark.graphx._

object PRGraphX  {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("PRGraphX"))
    val followerGraph = GraphLoader.edgeListFile(sc, "file:///home/wangyang/Documents/relation.txt")

    val initGraph = followerGraph.mapVertices{
      case(id,attr) => (0.1) //initial pr
    }

    var pagerankGraph = initGraph.outerJoinVertices(followerGraph.outDegrees) {
      (id, data, deg) => (data,deg.getOrElse(1))
    }.cache()

    var flag = true //exit loop when flag is false
    var tol = 100d //tolerance

    //first init
    var followers: VertexRDD[(Int, Double)] = pagerankGraph.aggregateMessages[(Int, Double)](
      triplet => {
        triplet.sendToDst(1, triplet.srcAttr._1/triplet.srcAttr._2)
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
    pagerankGraph = pagerankGraph.joinVertices(followers){
      case(id, data, pr) => (pr._2, data._2)
    }

    //while loop
    while(flag){
      var lastFollowers = followers
      followers = pagerankGraph.aggregateMessages[(Int, Double)](
        triplet => {
          triplet.sendToDst(1, triplet.srcAttr._1/triplet.srcAttr._2)
        },
        (a, b) => (a._1 + b._1, a._2 + b._2)
      )
      pagerankGraph = pagerankGraph.joinVertices(followers){
        case(id, data, pr) => (pr._2, data._2)
      }

      var nowDiff = pr_diff(lastFollowers.values.collectAsMap(), followers.values.collectAsMap())
      if(tol >= nowDiff)
        flag = false
    }
    pagerankGraph.vertices.top(5)(Ordering.by(_._2._1))foreach(println(_))

    def pr_diff(a: scala.collection.Map[Int, Double], b: scala.collection.Map[Int, Double]): Double = {
      var nowDiff: Double = 0d

      for ((k,v) <- a){
        if(v > b.get(k).get)
          nowDiff  += (v - b.get(k).get)
        else
          nowDiff += (b.get(k).get - v)
      }
      nowDiff
    }

  }

}