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

class HyperlinkInducedTopicSearchSuite extends SparkFunSuite with LocalSparkContext {
  test("Star StaticHyperlinkInducedTopicSearch") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()

      val staticHITS1 = starGraph.staticHyperlinkInducedTopicSearch(numIter = 1).vertices
      val staticHITS2 = starGraph.staticHyperlinkInducedTopicSearch(numIter = 2).vertices.cache()

      // Static HyperlinkInducedTopicSearch should only take 2 iteration to converge
      val notMatching = staticHITS1.innerZipJoin(staticHITS2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)
    }
  }  // end of test Star StaticHyperlinkInducedTopicSearch

  test("Chain StaticHyperlinkInducedTopicSearch") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()

      val staticHITS1 = chain.staticHyperlinkInducedTopicSearch(numIter = 1).vertices
      val staticHITS2 = chain.staticHyperlinkInducedTopicSearch(numIter = 2).vertices.cache()

      // Static HyperlinkInducedTopicSearch should only take 2 iteration to converge
      val notMatching = staticHITS1.innerZipJoin(staticHITS2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)
    }
  }  // end of test Chain StaticHyperlinkInducedTopicSearch
}

