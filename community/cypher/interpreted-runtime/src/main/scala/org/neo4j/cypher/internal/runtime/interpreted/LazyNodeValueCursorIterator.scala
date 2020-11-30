package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.values.virtual.NodeValue

import scala.collection.Iterator

// TAG: Lazy Implementation
abstract class LazyNodeValueCursorIterator extends CursorIterator[NodeValue] {

  /* Lazy additions */

  // Stride Additions
  //private val _strideSize: Int = 1
  //private var _currentcount: Int = 0

  // Batching (cached) additions
  var _cached : NodeValue = _
  var _useCached : Boolean = false
  def setUseCached(useCached : Boolean) : Unit = {
    this._useCached = useCached
  }

  protected def fetchNext(): NodeValue

  protected def close(): Unit

  override def hasNext: Boolean = _next != null || _useCached

  override def next(): NodeValue = {
    if (!hasNext) {
      close()
      Iterator.empty.next()
    }

    /*
    if(_currentcount == _strideSize) {
      // Finished exploring stride size
      _currentcount = 0
      return null
    }
    */

    if(_useCached) {
      //_currentcount += 1
      /*
      if(_next == null) {
        _useCached = false
        //_currentcount = 0
      }
      */
      return _cached
    }

    val current = _next
    _cached = current
    _next = fetchNext()
   // _currentcount += 1
    if (!hasNext) {
    //  _currentcount = 0
      close()
    }
    current
  }


}
