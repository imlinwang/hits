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

import math._

// Load the graph from the edge list file
val graph = GraphLoader.edgeListFile(sc, "karate.edgelist")
val num_node = graph.vertices.distinct.count.toLong

// Parameters
val num_iter = 30
val dominant_sv = sqrt(num_node) //1.0

// Compute the in- and out-neighbors of every vertex
val in_nbrs_set = graph.collectNeighbors(EdgeDirection.In).collect.map {
    case (vId, inNbrs) => (vId -> inNbrs.map(_._1)) } toMap //TODO: check warning
val out_nbrs_set = graph.collectNeighbors(EdgeDirection.Out).collect.map {
    case (vId, outNbrs) => (vId -> outNbrs.map(_._1)) } toMap //TODO: check warning

// Initialize
var hubs = out_nbrs_set.map { case (vId,nbrs) => (vId -> 1.0) } //Array.fill(num_node.toInt)(1.0)
var auths = in_nbrs_set.map { case (vId,nbrs) => (vId -> 1.0) } //Array.fill(num_node.toInt)(1.0)

// Iterate num_iter rounds
for (iter <- 1 to num_iter) {
    // Update and normalize the authority values
    auths = in_nbrs_set.map { case(vId,inNbrs) =>
        (vId -> inNbrs.map(nbr => hubs(nbr)).sum / dominant_sv) }
    // Update and normalize the hub values
    hubs = out_nbrs_set.map { case(vId,outNbrs) =>
        (vId -> outNbrs.map(nbr => auths(nbr)).sum / dominant_sv) }
}

hubs.foreach(println)
auths.foreach(println)
