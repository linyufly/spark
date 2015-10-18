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

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * Hyperlink-Induced Topic Search (HITS) algorithm implementation using the standalone
 * [[Graph]] interface and running for a fixed number of iterations, where each vertex
 * attribute has (hub, authority) as a pair of Double:
 * {{{
 * var HITS = Array.fill(n)((1.0, 1.0))
 * var oldHITS = Array.fill(n)((1.0, 1.0))
 * for( iter <- 0 until numIter ) {
 *   swap(oldHITS, HITS)
 *   for( i <- 0 until n) {
 *     HITS[i] = (
 *         hub = outNbrs[i].map(j => oldHITS[j].auth).sum,
 *         auth = inNbrs[i].map(j => oldHITS[j].hub).sum)
 *   }
 *   normalize(HITS)
 * }
 * }}}
 *
 * The original paper is "Authoritative sources in a hyperlinked environment" by Jon
 * Kleinberg (1999) published in the Journal of the ACM 46 (5): 604-632.
 */

object HyperlinkInducedTopicSearch extends Logging {
  case class Score(hub: Double, auth: Double)

  /**
   * Run HITS for a fixed number of iterations returning a graph with vertex attributes
   * containing (hub, authority) and null edge attributes.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing with each vertex containing (hub, authority) and
   *         null edge attributes.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[Score, Null] = {
    // Initialize the HubAuth graph with null edge attribute and each vertex with
    // (hub = 1.0, auth = 1.0).
    var hubAuthGraph: Graph[Score, Null] = graph
        .mapVertices((_, _) => Score(hub = 1.0, auth = 1.0)).mapEdges(e => null).cache()

    var iteration = 0
    var prevHubAuthGraph: Graph[Score, Null] = null

    while (iteration < numIter) {
      // Update the hub score from outgoing edges.
      val hubUpdates = hubAuthGraph.aggregateMessages[Double](
          ctx => {
            ctx.sendToSrc(ctx.dstAttr.auth)
          },
          _ + _,
          TripletFields.Dst
      )

      prevHubAuthGraph = hubAuthGraph
      hubAuthGraph = hubAuthGraph.mapVertices((_, _) => Score(hub = 0.0, auth = 0.0))
          .joinVertices(hubUpdates) {
        (id, oldScore, newHub) => Score(hub = newHub, auth = oldScore.auth)
      }.cache()
      hubAuthGraph.vertices.count  // materialization
      prevHubAuthGraph.vertices.unpersist(false)

      // Update the auth score from incoming edges.
      val authUpdates = hubAuthGraph.aggregateMessages[Double](
          ctx => {
            ctx.sendToDst(ctx.srcAttr.hub)
          },
          _ + _,
          TripletFields.Src
      )

      prevHubAuthGraph = hubAuthGraph
      hubAuthGraph = hubAuthGraph.mapVertices((id, score) => Score(hub = score.hub, auth = 0.0))
          .joinVertices(authUpdates) {
        (id, oldScore, newAuth) => Score(hub = oldScore.hub, auth = newAuth)
      }.cache()
      hubAuthGraph.vertices.count  // materialization
      prevHubAuthGraph.vertices.unpersist(false)

      logInfo(s"HyperlinkInducedTopicSearch finished iteration $iteration.")

      iteration += 1
    }

    // norm is used for normalization, which is (hub norm, auth norm).
    var norm = hubAuthGraph.vertices.map(v => v._2)
        .map(p => Score(hub = p.hub * p.hub, auth = p.auth * p.auth))
        .reduce((a, b) => Score(hub = a.hub + b.hub, a.auth + b.auth))

    // norm becomes (0.0, 0.0) iff the graph has no edges, and it should be set to (1.0, 1.0)
    // to prevent division by zero. In other cases, both norm.hub and norm.auth should be
    // larger than zero, as the initial hub and auth is set to 1 for every vertex.
    norm = if (norm.hub + norm.auth == 0.0) {
      Score(hub = 1.0, auth = 1.0)
    } else {
      Score(hub = math.sqrt(norm.hub), auth = math.sqrt(norm.auth))
    }

    // Normalization
    prevHubAuthGraph = hubAuthGraph
    hubAuthGraph = hubAuthGraph.mapVertices(
        (id, score) => Score(hub = score.hub / norm.hub, auth = score.auth / norm.auth)).cache()
    hubAuthGraph.vertices.count  // materialization
    prevHubAuthGraph.vertices.unpersist(false)

    hubAuthGraph
  }
}
