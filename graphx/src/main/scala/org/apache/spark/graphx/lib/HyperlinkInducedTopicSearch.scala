/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.graphx.lib

import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * HyperlinkInducedTopicSearch algorithm implementation using the standalone [[Graph]]
 * interface and running for a fixed number of iterations:
 * {{{
 * var HITS = Array.fill(n)((1.0, 1.0))
 * var oldHITS = Array.fill(n)((1.0, 1.0))
 * for( iter <- 0 until numIter ) {
 * swap(oldHITS, HITS)
 * for( i <- 0 until n) {
 * HITS[i] = (outNbrs[i].map(j => oldHITS[j]._2).sum,
 * inNbrs[i].map(j => oldHITS[j]._1).sum)
 * }
 * normalize(HITS)
 * }
 * }}}
 *
 * Note that each vertex attribute has (hub, authority) as a pair of Double.
 */

object HyperlinkInducedTopicSearch extends Logging {
  /**
   * Run HyperlinkInducedTopicSearch for a fixed number of iterations returning
   * a graph with vertex attributes containing (hub, authority) and null edge
   * attributes.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HyperlinkInducedTopicSearch
   * @param numIter the number of iterations of HyperlinkInducedTopicSearch to run
   *
   * @return the graph containing with each vertex containing (hub, authority) and
   *         null edge attributes.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Null] = {
    // Initialize the HubAuth graph with null edge attribute and each vertex with
    // (1.0, 1.0), where the first is hub score, and the second is auth score.
    var hubAuthGraph: Graph[(Double, Double), Null] = graph
        .mapVertices((_, _) => (1.0, 1.0)).mapEdges(e => null)

    var iteration = 0
    var prevHubAuthGraph: Graph[(Double, Double), Null] = null

    while (iteration < numIter) {
      hubAuthGraph.cache()

      // Compute the sum of hub score from incoming edges and the sum of auth score
      // from outgoing edges for each vertex.
      var hubAuthUpdates = hubAuthGraph.aggregateMessages[(Double, Double)](
          ctx => {
            ctx.sendToSrc(ctx.dstAttr._2, 0.0)
            ctx.sendToDst(0.0, ctx.srcAttr._1)
          },
          (a, b) => (a._1 + b._1, a._2 + b._2)
      )

      // norm is used for normalization, which is (hub norm, auth norm).
      var norm = hubAuthUpdates.map(v => v._2)
          .map(p => (p._1 * p._1, p._2 * p._2))
          .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      // If norm is (0.0, 0.0), set it to (1.0, 1.0) to prevent division by zero.
      norm = if (norm._1 + norm._2 == 0.0) {
        (1.0, 1.0)
      } else {
        (Math.sqrt(norm._1), Math.sqrt(norm._2))
      }

      // Apply the final (hub score, auth score) updates to get the new HubAuth,
      // using join to preserve scores of vertices that did not receive a message.
      prevHubAuthGraph = hubAuthGraph
      hubAuthGraph = hubAuthGraph.joinVertices(hubAuthUpdates) {
        (id, oldSum, newSum) => (newSum._1 / norm._1, newSum._2 / norm._2)
      }.cache()

      hubAuthGraph.vertices.foreachPartition(v => {})  // materialization
      hubAuthGraph.edges.foreachPartition(e => {})  // materialization

      logInfo(s"HyperlinkInducedTopicSearch finished iteration $iteration.")

      prevHubAuthGraph.vertices.unpersist(false)
      prevHubAuthGraph.edges.unpersist(false)

      iteration += 1
    }

    hubAuthGraph
  }
}
