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

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

// Load the graph from the edge list file
val graph = GraphLoader.edgeListFile(sc, "karate.edgelist")
val num_vertex = graph.vertices.count().toInt

// Compute the in- and out-neighbors of every vertex
val in_nbrs = graph.collectNeighbors(EdgeDirection.In)
val out_nbrs = graph.collectNeighbors(EdgeDirection.Out)

// Parameters
val num_iter = 10
val dominant_eigen = 1.0

// Initialize
var hubs = Array.fill(num_vertex)(1.0)
var auths = Array.fill(num_vertex)(1.0)

// Iterate num_iter rounds
for (iter <- 0 until num_iter) {
    // Update and normalize the authority values
    for (i <- 0 unitl num_vertex) {
        auths(i) = in_nbrs[i].map(j => hubs(j)).sum / dominant_eigen
    }
    // Update and normalize the hub values
    for (i <- 0 until num_vertex) {
        hubs(i) = out_nbrs[i].map(j => auths(j)).sum / dominant_eigen
    }
}

hubs.collect.foreach(println)
auths.collect.foreach(println)
