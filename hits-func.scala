//
// Copyright (c) 2015 Lin Wang
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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._


object Hits extends Logging {
    /**
     * Run Hits for a fixed number of iterations returning a graph with vertex
     * attributes containing the hub and authority
     * @param graph the graph on which to compute Hits
     * @param numIter the number of iterations of Hits to Run
     * @param degreeThreshold the degree threshold to prone the graph
     *
     * @return the graph containing with each vertex containing the normalized
     * hub and authority
    */

    def run[VD: ClassTag, ED: ClassTag](
        graph: Graph[VD, ED], numIter: Int, degreeThreshold: Int = 0):
        Graph[Double, Double] =
    {
        // The subgraph for estimating the eigenvalue
        val subgraph = graph.outerJoinVertices(graph.degrees){
            case(id, attr, deg) => deg
        }.subgraph(vpred = (id, attr) => attr.get >= degreeThreshold)

        // Obtain the edge list with weight 1.0 on each edge
        val edge_list = subgraph.triplets.map(
            triplet => (triplet.srcId.toInt, triplet.dstId.toInt, (1).toDouble)
        )

        // Generate sparse rows for RowMatrix
        val rows = edge_list.groupBy(_._1).map[(Int, SparseVector)] {
            row => val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
            (row._1, new SparseVector(
                subgraph.vertices.map(_._1).collect.distinct.size.toInt,
                indices.toArray, values.toArray))
        }

        // Generate the RowMatrix
        val mat = new RowMatrix(rows.map[Vector](_._2).persist())

        // Compute the Gramian Matrix for the RowMatrix
        val mat_gramian = mat.computeGramianMatrix()

        // Transform the gramian Matrix to a distributed RowMatrix
        val rows = mat_gramian.transpose.toArray.grouped(mat_gramian.numRows).toArray
        val vectors = rows.map(row => new DenseVector(row))
        val rows_gramian: RDD[Vector] = sc.parallelize(vectors)
        val mat_gramian_distr = new RowMatrix(rows_gramian)

        // Apply the SVD function to compute the dominant singular value
        val svd: SingularValueDecomposition[RowMatrix, Matrix]
            = mat_gramian_distr.computeSVD(1, computeU = false)
        val dominant_sv = svd.s.apply(0)

        var hitsGraph: Graph[(Double, Double), Double] = graph.mapVertices(
            (id, attr) => (1.0, 1.0)).mapEdges(e => e.attr.toDouble)

        var iterations = 0
        var prevHitsGraph: Graph[(Double, Double), Double] = null
        while (iteration < numIter) {
            hitsGraph.cache()

            // Compute the authority contributions of each vertex, perform local
            // preaggregation, and do the final aggregation at the receiving
            // vertices. Requires a shuffle for aggregation.
            val authUpdates = hitsGraph.mapReduceTriplets[Double](
                // Map function
                triplet => Iterator(triplet.dstId, triplet.srcAttr._2.toDouble),
                // Reduce function
                (a, b) => a + b
            )


            // Apply the final authrity update to get the new authorities, using
            // join to preserve authorities of vertices that didn't receive a
            // message. Requires a shuffle for broadcasting updated authorities
            // to the edge partitions.
            prevHitsGraph = hitsGraph
            hitsGraph = hitsGraph.joinVertices(authUpdates) {
                (id, oldValues, msgSum)
                => (msgSum / dominant_sv, oldValues._2)
            }.cache()
            // materializes hitsGraph.vertices
            hitsGraph.edges.foreachPartition(x => {})
            prevHitsGraph.vertices.unpersist(false)
            prevHitsGraph.edges.unpersist(false)

            logInfo(s"Authorities updated")

            // Compute the hub contributions of each vertex, perform local
            // preaggregation, and do the final aggregation at the receiving
            // vertices. Requires a shuffle for aggregation.
            val hubUpdates = hitsGraph.mapReduceTriplets[Double](
                // Map function
                triplet => Iterator(triplet.dstId, triplet.srcAttr._1.toDouble),
                // Reduce function
                (a, b) => a + b
            )

            // Apply the final hub update to get the new hubs, using
            // join to preserve hubs of vertices that didn't receive a
            // message. Requires a shuffle for broadcasting updated hubs
            // to the edge partitions.
            prevHitsGraph = hitsGraph
            hitsGraph = hitsGraph.joinVertices(hubUpdates) {
                (id, oldValues, msgSum)
                => (oldValues._1, msgSum / dominant_sv)
            }.cache()
            // materializes hitsGraph.vertices
            hitsGraph.edges.foreachPartition(x => {})
            prevHitsGraph.vertices.unpersist(false)
            prevHitsGraph.edges.unpersist(false)

            logInfo(s"Hubs updated")

            logInfo(s"HITS finished iteration $iteration.")

            iteration += 1
        }
        hitsGraph
    }
}
