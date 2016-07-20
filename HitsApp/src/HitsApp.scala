//
// Copyright (c) 2016 Lin Wang
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._

object Hits {

  // Given a directed graph and a degree threshold, creates a subgraph with those vertices
  // with at least such degree and computes the SVD on its adj matrix and its transpose.
  // It returns the maximum computed SV for both matrices.
  //     (Graph, Int) => (Double, Double)
  // (graph, degThrs) -> (maxSV_auth, maxSV_hub)
  def getApproxMaxSVD (graph: Graph[Int, Int], degreeThreshold: Int): (Double, Double) = {
    val subgraph = graph.outerJoinVertices(graph.degrees){
      case(id, attr, deg) => deg
    }.subgraph(vpred = (id, attr) => attr.get >= degreeThreshold)
    val numNodeSub = subgraph.vertices.map(vertex => vertex._1).collect.distinct.length.toInt
    // Obtain the edge list with weight 1.0 on each edge
    val edgeList = subgraph.triplets.map(
      triplet => (triplet.srcId.toInt, triplet.dstId.toInt, (1).toDouble)
    )
    // Generate sparse rows for RowMatrix
    val rowsRaw_t = edgeList.groupBy(_._2).map[(Int, SparseVector)] {
      row => val (indices, values) = row._2.map(e => (e._1, e._3)).unzip
        (row._1, new SparseVector(
          numNodeSub, indices.toArray, values.toArray))
    }
    val rowsRaw = edgeList.groupBy(_._1).map[(Int, SparseVector)] {
      row => val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
        (row._1, new SparseVector(
          numNodeSub, indices.toArray, values.toArray))
    }
    // Generate the RowMatrix (transpose)
    val buffRDD_t = rowsRaw_t.map[Vector](_._2).persist()
    val mat_t = new RowMatrix(buffRDD_t)
    // Apply the SVD function to compute the dominant singular value (transpose)
    val svd_t: SingularValueDecomposition[RowMatrix, Matrix] = mat_t.computeSVD(1, computeU = false)
    buffRDD_t.unpersist(false)
    // Generate the RowMatrix
    val buffRDD = rowsRaw.map[Vector](_._2).persist()
    val mat = new RowMatrix(buffRDD)
    // Apply the SVD function to compute the dominant singular value
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(1, computeU = false)
    buffRDD.unpersist(false)
    //val dominantSV = svd.s.apply(0)
    (svd_t.s.apply(0), svd.s.apply(0))
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: org.leihn.Hits <master> <graph> <threshold> <iteration>")
    }

    // Set up Spark context
    val conf = new SparkConf().setAppName("Hits").setMaster(args.apply(0).toString)
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, args.apply(1).toString)
    val numNode = graph.vertices.map(vertex => vertex._1).collect.distinct.length.toInt

    val degreeThreshold = args.apply(2).toInt
    // Compute the dominant SV for both M^t and M (M=Adj(G))
    // In general, sqrt(\lambda_\max) for M^tM = \sigma_\max for M
    val dominantSV = getApproxMaxSVD(graph, degreeThreshold)

    // Initialize
    var hitsGraph: Graph[(Double, Double), Double] = graph.mapVertices(
      (id, attr) => (0.0, 0.0)).mapEdges(e => e.attr.toDouble)

    hitsGraph = hitsGraph.joinVertices(graph.inDegrees) {
      // implicit first iteration
      (id, hits, degOpt) => (degOpt.toDouble / math.sqrt(numNode) / dominantSV._1, hits._2)
      //(id, hits, degOpt) => (degOpt.toDouble, hits._2)
    }.joinVertices(graph.outDegrees) {
      // implicit first iteration
      (id, hits, degOpt) => (hits._1, degOpt.toDouble / math.sqrt(numNode) / dominantSV._2)
      //(id, hits, degOpt) => (hits._1, degOpt.toDouble)
    }

    val numIter = args.apply(3).toInt

    var iterations = 0
    var prevHitsGraph: Graph[(Double, Double), Double] = null
    while (iterations < numIter) {
      hitsGraph.cache()

      // Compute the authority contributions of each vertex, perform local
      // preaggregation, and do the final aggregation at the receiving
      // vertices. Requires a shuffle for aggregation.
      val authUpdates = hitsGraph.aggregateMessages[Double](
        sendMsg = {
          triplet => triplet.sendToSrc(triplet.dstAttr._2.toDouble)
        },
        mergeMsg = {
          (a, b) => a + b
        }
      )

      // Apply the final authority update to get the new authorities and
      // normalize them. It uses join to preserve authorities of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting
      // updated authorities to the edge partitions.
      prevHitsGraph = hitsGraph
      hitsGraph = hitsGraph.joinVertices(authUpdates) {
        (id, oldValues, msgSum) => (msgSum / dominantSV._1, oldValues._2)
      }.mapEdges(e => e.attr.toDouble).cache()

      // materializes hitsGraph.vertices
      hitsGraph.edges.foreachPartition(x => {})
      prevHitsGraph.vertices.unpersist(false)
      prevHitsGraph.edges.unpersist(false)

      println("Authorities updated")

      // Compute the hub contributions of each vertex, perform local
      // preaggregation, and do the final aggregation at the receiving
      // vertices. Requires a shuffle for aggregation.
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        sendMsg = {
          triplet => triplet.sendToDst(triplet.srcAttr._1.toDouble)
        },
        mergeMsg = {
          (a, b) => a + b
        }
      )

      // Apply the final hub update to get the new hubs and normalize
      // them. It uses join to preserve hub of vertices that didn't
      // receive a message. Requires a shuffle for broadcasting updated
      // hubs to the edge partitions.
      prevHitsGraph = hitsGraph
      hitsGraph = hitsGraph.joinVertices(hubUpdates) {
        (id, oldValues, msgSum)
        => (oldValues._1, msgSum / dominantSV._2)
      }.mapEdges(e => e.attr.toDouble).cache()

      // materializes hitsGraph.vertices
      hitsGraph.edges.foreachPartition(x => {})
      prevHitsGraph.vertices.unpersist(false)
      prevHitsGraph.edges.unpersist(false)

      println("Hubs updated")

      iterations += 1
    }

    // Final normalization
    val authNorm = math.sqrt(hitsGraph.vertices.map{ case (vid, attr) => math.pow(attr._1, 2.0) } sum)
    val hubsNorm = math.sqrt(hitsGraph.vertices.map{ case (vid, attr) => math.pow(attr._2, 2.0) } sum)

    hitsGraph = hitsGraph.mapVertices(
      (id, attr) => (attr._1 / authNorm, attr._2 / hubsNorm)
    ).mapEdges(e => e.attr.toDouble)

    hitsGraph.vertices.saveAsTextFile("./hits.result")
  }
}
