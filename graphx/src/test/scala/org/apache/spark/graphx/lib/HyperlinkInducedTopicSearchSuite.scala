/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object GridHyperlinkInducedTopicSearch {
  def apply(nRows: Int, nCols: Int, nIter: Int):
      Seq[(VertexId, HyperlinkInducedTopicSearch.Score)] = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        inNbrs(sub2ind(r + 1, c)) += ind
        outNbrs(ind) += sub2ind(r + 1, c)
      }
      if (c + 1 < nCols) {
        inNbrs(sub2ind(r, c + 1)) += ind
        outNbrs(ind) += sub2ind(r, c + 1)
      }
    }
    // Compute the HITS
    var score = Array.fill(nRows * nCols)(HyperlinkInducedTopicSearch.Score(hub = 1.0, auth = 1.0))
    for (iter <- 0 until nIter) {
      var newHub = Array.fill(nRows * nCols)(0.0)
      for (ind <- 0 until (nRows * nCols)) {
        newHub(ind) = outNbrs(ind).map(nbr => score(nbr).auth).sum
      }
      var newAuth = Array.fill(nRows * nCols)(0.0)
      for (ind <- 0 until (nRows * nCols)) {
        newAuth(ind) = inNbrs(ind).map(nbr => newHub(nbr)).sum
      }
      for (ind <- 0 until (nRows * nCols)) {
        score(ind) = HyperlinkInducedTopicSearch.Score(hub = newHub(ind), auth = newAuth(ind))
      }
    }

    var sumHub = 0.0
    var sumAuth = 0.0
    for (ind <- 0 until (nRows * nCols)) {
      sumHub += score(ind).hub * score(ind).hub
      sumAuth += score(ind).auth * score(ind).auth
    }
    sumHub = if (sumHub == 0.0) 1.0 else math.sqrt(sumHub)
    sumAuth = if (sumAuth == 0.0) 1.0 else math.sqrt(sumAuth)

    for (ind <- 0 until (nRows * nCols)) {
      score(ind) = HyperlinkInducedTopicSearch.Score(
          score(ind).hub / sumHub, score(ind).auth / sumAuth)
    }

    (0L until (nRows * nCols)).zip(score)
  }
}

class HyperlinkInducedTopicSearchSuite extends SparkFunSuite with LocalSparkContext {
  val tolerance = 1e-8

  def sign(value: Double): Int = {
    if (value < -tolerance)
      -1
    else if (value > tolerance)
      1
    else
      0
  }

  def equal(a: Double, b: Double): Boolean = { sign(a - b) == 0 }

  test("Star StaticHyperlinkInducedTopicSearch") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices)

      // Static HyperlinkInducedTopicSearch should only take 2 iteration to converge
      val staticHITS1 = starGraph.staticHyperlinkInducedTopicSearch(numIter = 1).vertices
      val staticHITS2 = starGraph.staticHyperlinkInducedTopicSearch(numIter = 2).vertices

      val comparison =
          staticHITS1.innerZipJoin(staticHITS2){ (vid, pr1, pr2) => (pr1, pr2) }.collect()
      comparison.foreach{ v =>
        assert(v._2._1 === v._2._2)
        assert(
            v._1 == 0 && v._2._1.hub == 0.0 && v._2._1.auth == 1.0 ||
            v._1 > 0 && equal(
                v._2._1.hub, math.sqrt(1.0 / (nVertices - 1))) && v._2._1.auth == 0.0)
      }
    }
  }  // end of test Star StaticHyperlinkInducedTopicSearch

  test("Chain StaticHyperlinkInducedTopicSearch") {
    withSpark { sc =>
      val nVertices = 10
      val chain1 = (0 until nVertices - 1).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map{ case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0)

      // Static HyperlinkInducedTopicSearch should only take 2 iteration to converge
      val staticHITS1 = chain.staticHyperlinkInducedTopicSearch(numIter = 1).vertices
      val staticHITS2 = chain.staticHyperlinkInducedTopicSearch(numIter = 2).vertices

      val comparison =
          staticHITS1.innerZipJoin(staticHITS2){ (vid, pr1, pr2) => (pr1, pr2) }.collect()
      comparison.foreach{ v =>
        assert(v._2._1 === v._2._2)
        assert(
            v._1 == nVertices - 1 && v._2._1.hub == 0.0
            && equal(v._2._1.auth, math.sqrt(1.0 / (nVertices - 1))) ||
            v._1 > 0 && v._1 + 1 < nVertices && equal(v._2._1.hub, v._2._1.auth)
            && equal(v._2._1.auth, math.sqrt(1.0 / (nVertices - 1))) ||
            v._1 == 0 && v._2._1.auth == 0.0
            && equal(v._2._1.hub, math.sqrt(1.0 / (nVertices - 1))))
      }
    }
  }  // end of test Chain StaticHyperlinkInducedTopicSearch

  test("Grid StaticHyperlinkInducedTopicSearch") {
    withSpark { sc =>
      val rows = 5
      val cols = 5
      val numIter = 3
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols)
      val staticHITS = gridGraph.staticHyperlinkInducedTopicSearch(numIter).vertices
      val referenceHITS = VertexRDD(
        sc.parallelize(GridHyperlinkInducedTopicSearch(rows, cols, numIter)))
      val comparison =
          staticHITS.innerZipJoin(referenceHITS){ (vid, pr1, pr2) => (pr1, pr2) }.collect()
      comparison.foreach{ v =>
        assert(equal(v._2._1.hub, v._2._2.hub) && equal(v._2._1.auth, v._2._2.auth))
      }
    }
  }  // end of test Grid StaticHyperlinkInducedTopicSearch
}

