package org.neo4j.cypher.internal.runtime.interpreted

import scala.collection.Iterator

abstract class CursorIterator[T] extends Iterator[T] with AutoCloseable {
  // TAG: Lazy Implementation
  protected var _next: T = fetchNext()

  protected def fetchNext(): T

  protected def close(): Unit

  override def hasNext: Boolean = _next != null

  override def next(): T = {
    if (!hasNext) {
      close()
      Iterator.empty.next()
    }

    val current = _next
    _next = fetchNext()
    if (!hasNext) {
      close()
    }
    current
  }
}