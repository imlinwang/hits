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
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._


/* Spark - GraphX */
// Load the graph from the edge list file
val graph = GraphLoader.edgeListFile(sc, "karate.edgelist")

// Compute the pagerank of the graph
val ranks = graph.pageRank(0.0001).vertices
println(ranks.collect.mkString("\n"))


/* Spark */
// Read the graph file
val inputData = sc.textFile("karate.edgelist").map {
  line => val parts = line.split(" ")
  (parts(0).toLong, parts(1).toInt, (1).toDouble)
}

// Number of columns
val nCol = inputData.flatMap(e => List(e._1, e._2)).distinct.count.toInt

// Generate rows for RowMatrix
val dataRows = inputData.groupBy(_._1).map[(Long, SparseVector)] {
  row => val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
  (row._1, new SparseVector(nCol, indices.toArray, values.toArray))
}

// Generate the RowMatrix
val mat = new RowMatrix(dataRows.map[Vector](_._2).persist())

val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = false)

val s: Vector = svd.s

s.toArray.foreach(println)


print(dataRM)


// sc.makeRDD(svd.s.toArray, 1).saveAsTextFile("karate.sv")
