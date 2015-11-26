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
val num_node = graph.vertices.distinct.count.toLong

// Compute the in- and out-neighbors of every vertex
val in_nbrs_set = graph.collectNeighbors(EdgeDirection.In).collect
val out_nbrs_set = graph.collectNeighbors(EdgeDirection.Out).collect

// Parameters
val num_iter = 10
val dominant_sv = 1.0

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

hubs.foreach(println)
auths.foreach(println)
