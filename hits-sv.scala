//
// Copyright (c) 2015 Lin Wang, Luis F. Chiroque
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




import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._


/* Spark - GraphX */
// Load the graph from the edge list file
val graph = GraphLoader.edgeListFile(sc, "karate.edgelist")

// Get a subgraph filtering by a given degree threshold
val degreeThreshold = 3
/* first, degree is added (overriding) as a vertex attribute (in outerJoinVertices) */
/* note: the new degree attribute is added as Option[Int] */
val subgraph = graph.outerJoinVertices(graph.degrees){
    case(id, attr, deg) => deg
}.subgraph(vpred = (id, attr) => attr.get > degreeThreshold)

// Compute the pagerank of the graph
val ranks = graph.pageRank(0.0001).vertices
println(ranks.collect.mkString("\n"))

// Number of nodes in the subgraph
val num_node = subgraph.vertices.map(
    vertex => vertex._1
).collect.distinct.size.toLong

// Obtain all the triplets from the subgraph
// (src, dst, 1.0)
val edge_list = subgraph.triplets.map(
    triplet => (triplet.srcId.toLong, triplet.dstId.toLong, (1).toDouble)
)

// Generate sparse rows for RowMatrix
val rows = edge_list.groupBy(_._1).map[(Long, SparseVector)] {
    row => val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
    (row._1, new SparseVector(num_node, indices.toArray, values.toArray))
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

// Compute the in- and out-neighbors of every vertex
val in_nbrs_set = graph.collectNeighbors(EdgeDirection.In).collect
val out_nbrs_set = graph.collectNeighbors(EdgeDirection.Out).collect

// Parameters
val num_iter = 50

// Initialize
var hubs = Array.fill(num_node.toInt)(1.0)
var auths = Array.fill(num_node.toInt)(1.0)

// Iterate num_iter rounds
for (iter <- 1 to num_iter) {
    // Update and normalize the authority values
    for (i <- 1 to num_node.toInt) {
        val in_nbrs = in_nbrs_set.find(_._1 == i).get._2.map(_._1)
        auths(i - 1) = in_nbrs.map(j => hubs(j.toInt - 1)).sum / dominant_sv
    }
    // Update and normalize the hub values
    for (i <- 1 to num_node.toInt) {
        val out_nbrs = out_nbrs_set.find(_._1 == i).get._2.map(_._1)
        hubs(i - 1) = out_nbrs.map(j => auths(j.toInt - 1)).sum / dominant_sv
    }
}

// Print the results for hubs and authorities
hubs.foreach(println)
auths.foreach(println)
