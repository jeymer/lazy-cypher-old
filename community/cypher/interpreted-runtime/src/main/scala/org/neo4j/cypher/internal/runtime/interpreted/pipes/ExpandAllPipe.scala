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

import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

case class ExpandAllPipe(source: Pipe,
                         fromName: String,
                         relName: String,
                         toName: String,
                         dir: SemanticDirection,
                         types: RelationshipTypes)
                        (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row => {
        // TAG: Lazy Implementation
        if(row == null) {
          None
        }
        else {
          row.getByName(fromName) match {
            case n: NodeValue =>
              val relationships: Iterator[RelationshipValue] = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
              relationships.map { r =>
                val other = r.otherNode(n)
                executionContextFactory.copyWith(row, relName, r, toName, other)
              }
            case IsNoValue() => None

            case value => throw new ParameterWrongTypeException(s"Expected to find a node at '$fromName' but found $value instead")
          }
        }
      }
    }
  }
}
