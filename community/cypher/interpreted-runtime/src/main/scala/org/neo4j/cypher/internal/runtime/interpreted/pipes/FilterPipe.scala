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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class FilterPipe(source: Pipe, predicate: Expression)
                     (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  predicate.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    // TAG: Lazy Implementation
    /*
    input.filter(ctx => {
      (ctx == null) || (predicate(ctx, state) eq Values.TRUE)
    })
    */

    // Build filter with a map
    // Essentially, we don't want the filter pipe to keep pulling until it finds an element that passes
    // We want it to only check one at a time, so all filters in a batch are still at the same element
    // Therefore, if you pull one which doesn't match, then just pass along null
    input.map(ctx => {
      if(predicate(ctx, state) eq Values.TRUE) {
        ctx
      } else {
        null
      }
    })
  }
}
