/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.virtual.NodeValue

case class NodeByLabelScanPipe(ident: String, label: LazyLabel)
                              (val id: Id = Id.INVALID_ID) extends Pipe  {

  // TAG: Lazy Implementation
  var nodes : Iterator[NodeValue] = _
  def setNodes(iterator: Iterator[NodeValue]) : Unit = { nodes = iterator }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {

    val id = label.getId(state.query)
    if (id != UNKNOWN) {
      if(nodes == null) {
        setNodes(state.query.getNodesByLabel(id))
      }
      val baseContext = state.newExecutionContext(executionContextFactory)
      nodes.map(n => {
        // TAG: Lazy Implementation
        if (n == null) {
          null
        }
        else {
          executionContextFactory.copyWith(baseContext, ident, n)
        }
      })
    } else Iterator.empty
  }
}
