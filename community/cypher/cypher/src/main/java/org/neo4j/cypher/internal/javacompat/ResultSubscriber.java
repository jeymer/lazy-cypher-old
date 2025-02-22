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
package org.neo4j.cypher.internal.javacompat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.NonFatalCypherError;
import org.neo4j.cypher.internal.result.ClosingExecutionResult;
import org.neo4j.cypher.internal.result.StandardInternalExecutionResult;
import org.neo4j.cypher.internal.result.string.ResultStringBuilder;
import org.neo4j.cypher.internal.runtime.interpreted.LazyNodeValueCursorIterator;
import org.neo4j.cypher.internal.runtime.interpreted.PipeExecutionResult;
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllNodesScanPipe;
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeByLabelScanPipe;
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe;
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.AnyValue;

/**
 * A {@link QuerySubscriber} that implements the {@link Result} interface.
 * <p>
 * This implementation wraps a {@link QueryExecution} and presents both an iterator-based API and a visitor-based API
 * using the underlying {@link QueryExecution} to serve the results.
 */
public class ResultSubscriber extends PrefetchingResourceIterator<Map<String,Object>> implements QuerySubscriber, Result
{
    private final DefaultValueMapper valueMapper;
    private final TransactionalContext context;
    QueryExecution execution;
    private AnyValue[] currentRecord;
    private Throwable error;
    private QueryStatistics statistics;
    private ResultVisitor<?> visitor;
    private Exception visitException;
    private List<Map<String,Object>> materializeResult;
    private Iterator<Map<String,Object>> materializedIterator;
    private int currentOffset = -1;

    public ResultSubscriber( TransactionalContext context )
    {
        this.context = context;
        this.valueMapper = new DefaultValueMapper( context.transaction() );
    }

    public void init( QueryExecution execution )
    {
        this.execution = execution;
        assertNoErrors();
    }

    public void materialize( QueryExecution execution )
    {
       this.execution = execution;
       this.materializeResult = new ArrayList<>(  );
       fetchResults( Long.MAX_VALUE );
    }

    // QuerySubscriber part
    @Override
    public void onResult( int numberOfFields )
    {
        this.currentRecord = new AnyValue[numberOfFields];
    }

    @Override
    public void onRecord()
    {
        currentOffset = 0;
    }

    @Override
    public void onField( AnyValue value )
    {
        currentRecord[currentOffset++] = value;
    }

    @Override
    public void onRecordCompleted()
    {
        currentOffset = -1;
        //We are coming from a call to accept
        if ( visitor != null )
        {
            try
            {
                if ( !visitor.visit( new ResultRowImpl( createPublicRecord() ) ) )
                {
                    execution.cancel();
                    visitor = null;
                }
            }
            catch ( Exception exception )
            {
                this.visitException = exception;
            }
        }

        //we are materializing the result
        if ( materializeResult != null )
        {
            materializeResult.add( createPublicRecord() );
        }
    }

    @Override
    public void onError( Throwable throwable )
    {
        this.error = throwable;
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        this.statistics = statistics;
    }

    // Result part
    @Override
    public QueryExecutionType getQueryExecutionType()
    {
        try
        {
            return execution.executionType();
        }
        catch ( Throwable throwable )
        {
            close();
            throw converted( throwable );
        }
    }

    @Override
    public List<String> columns()
    {
        return Arrays.asList( execution.fieldNames() );
    }

    @Override
    public <T> ResourceIterator<T> columnAs( String name )
    {
        return new ResourceIterator<>()
        {
            @Override
            public void close()
            {
                ResultSubscriber.this.close();
            }

            @Override
            public boolean hasNext()
            {
                return ResultSubscriber.this.hasNext();
            }

            @SuppressWarnings( "unchecked" )
            @Override
            public T next()
            {
                Map<String,Object> next = ResultSubscriber.this.next();
                return (T) next.get( name );
            }
        };
    }

    @Override
    public void close()
    {
        execution.cancel();
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return statistics == null ? QueryStatistics.EMPTY : statistics;
    }

    @Override
    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        try
        {
            return execution.executionPlanDescription();
        }
        catch ( Exception e )
        {
            throw converted( e );
        }
    }

    @Override
    public String resultAsString()
    {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter( out );
        writeAsStringTo( writer );
        writer.flush();
        return out.toString();
    }

    @Override
    public void writeAsStringTo( PrintWriter writer )
    {
        ResultStringBuilder stringBuilder =
                ResultStringBuilder.apply( execution.fieldNames(), context );
        try
        {
            //don't materialize since that will close down the underlying transaction
            //and we need it to be open in order to serialize nodes, relationships, and
            //paths
            if ( this.hasFetchedNext() )
            {
                stringBuilder.addRow( new ResultRowImpl( this.getNextObject() ) );
            }
            accept( stringBuilder );
            stringBuilder.result( writer, statistics );
            for ( Notification notification : getNotifications() )
            {
                writer.println( notification.getDescription() );
            }
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return execution.getNotifications();
    }

    @Override
    public <VisitationException extends Exception> void accept( ResultVisitor<VisitationException> visitor )
            throws VisitationException
    {
        if ( isMaterialized() )
        {
            acceptFromMaterialized( visitor );
        }
        else if ( execution.isVisitable() )
        {
            this.statistics = execution.accept( visitor );
        }
        else
        {
            acceptFromSubscriber( visitor );
        }
        close();
    }

    @Override
    protected Map<String,Object> fetchNextOrNull()
    {
        if ( isMaterialized() )
        {
            return nextFromMaterialized();
        }
        else
        {
            return nextFromSubscriber();
        }
    }

    private Map<String,Object> nextFromMaterialized()
    {
        assertNoErrors();
        if ( materializedIterator == null )
        {
            materializedIterator = materializeResult.iterator();
        }
        if ( materializedIterator.hasNext() )
        {
            Map<String,Object> next = materializedIterator.next();
            if ( !materializedIterator.hasNext() )
            {
                close();
            }
            return next;
        }
        else
        {
            close();
            return null;
        }
    }

    private Map<String,Object> nextFromSubscriber()
    {
        fetchResults( 1 );
        assertNoErrors();
        if ( hasNewValues() )
        {
            HashMap<String,Object> record = createPublicRecord();
            markAsRead();
            return record;
        }
        else
        {
            close();
            return null;
        }
    }

    private boolean hasNewValues()
    {
        return currentRecord.length > 0 && currentRecord[0] != null;
    }

    private void markAsRead()
    {
        if ( currentRecord.length > 0 )
        {
            currentRecord[0] = null;
        }
    }

    private void fetchResults( long numberOfResults )
    {
        try
        {
            execution.request( numberOfResults );
            assertNoErrors();
            execution.await();
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    private HashMap<String,Object> createPublicRecord()
    {
        String[] fieldNames = execution.fieldNames();
        HashMap<String,Object> result = new HashMap<>();

        try
        {
            for ( int i = 0; i < fieldNames.length; i++ )
            {
                result.put( fieldNames[i], currentRecord[i].map( valueMapper ) );
            }
        }
        catch ( Throwable t )
        {
            throw converted( t );
        }
        return result;
    }

    private void assertNoErrors()
    {
        if ( error != null )
        {
            if ( NonFatalCypherError.isNonFatal( error ) )
            {
                close();
            }
            throw converted( error );
        }
    }

    private static QueryExecutionException converted( Throwable e )
    {
        Neo4jException neo4jException;
        if ( e instanceof Neo4jException )
        {
            neo4jException = (Neo4jException) e;
        }
        else if ( e instanceof RuntimeException )
        {
            throw (RuntimeException) e;
        }
        else
        {
            neo4jException = new CypherExecutionException( e.getMessage(), e );
        }
        return new QueryExecutionKernelException( neo4jException ).asUserException();
    }

    private <VisitationException extends Exception> void acceptFromMaterialized(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        assertNoErrors();
        for ( Map<String,Object> materialized : materializeResult )
        {
            if ( !visitor.visit( new ResultRowImpl( materialized ) ) )
            {
                break;
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    private <VisitationException extends Exception> void acceptFromSubscriber(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        this.visitor = visitor;
        fetchResults( Long.MAX_VALUE );
        if ( visitException != null )
        {
            throw (VisitationException) visitException;
        }
        assertNoErrors();
    }

    @VisibleForTesting
    public boolean isMaterialized()
    {
        return materializeResult != null;
    }

    // TAG: Lazy Implementation
    /* Jeff's Lazy Additions */
    private StringWriter lazyOut;
    private PrintWriter lazyWriter;

    @Override
    public String lazyResultAsString()
    {
        /* Initialize writers if first time */
        if(this.lazyWriter == null) {
            this.lazyOut = new StringWriter();
            this.lazyWriter = new PrintWriter( this.lazyOut );
        }

        boolean success = lazyWriteAsStringTo( this.lazyWriter );

        if(success) {
            this.lazyWriter.flush();
            return this.lazyOut.toString();
        }

        return null;
    }

    private ResultStringBuilder lazyStringBuilder;
    public boolean lazyWriteAsStringTo( PrintWriter writer )
    {
        if(this.lazyStringBuilder == null) {
            this.lazyStringBuilder =
                    ResultStringBuilder.apply( execution.fieldNames(), context );
        }

        try
        {
            //don't materialize since that will close down the underlying transaction
            //and we need it to be open in order to serialize nodes, relationships, and
            //paths
            if ( this.hasFetchedNext() )
            {
                this.lazyStringBuilder.addRow( new ResultRowImpl( this.getNextObject() ) );
            }
            boolean success = lazyAccept( this.lazyStringBuilder );
            if(success) {
                this.lazyStringBuilder.result( writer, statistics );
                for ( Notification notification : getNotifications() )
                {
                    writer.println( notification.getDescription() );
                }
            }
            return success;
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    public <VisitationException extends Exception> boolean lazyAccept( ResultVisitor<VisitationException> visitor )
            throws VisitationException
    {
        boolean success = false;
        if ( isMaterialized() )
        {
            acceptFromMaterialized( visitor );
            // TAG: Lazy Implementation
            // TODO: Should this be true? Should the query ever be materialized in the lazy system?
            success = true;
        }
        else if ( execution.isVisitable() )
        {
            this.statistics = execution.accept( visitor );
        }
        else
        {
            success = lazyAcceptFromSubscriber( visitor );
        }
        if(success) {
            close();
        }
        return success;
    }

    @SuppressWarnings( "unchecked" )
    private <VisitationException extends Exception> boolean lazyAcceptFromSubscriber(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        this.visitor = visitor;
        //boolean success = lazyFetchResults( Long.MAX_VALUE );
        boolean success = lazyFetchResults(1);
        if ( visitException != null )
        {
            throw (VisitationException) visitException;
        }
        assertNoErrors();
        return success;
    }

    private boolean lazyFetchResults( long numberOfResults )
    {
        try
        {
            boolean success = execution.lazyRequest( numberOfResults );
            assertNoErrors();
            // TAG: Lazy Implementation
            // execution.await();
            return success;
        }
        catch ( Exception e )
        {
            close();
            throw converted( e );
        }
    }

    // Batching methods
    public void initializeForBatching() {

        // Confirm proper types
        if(!(this.execution instanceof ClosingExecutionResult)) {
            System.out.println("Error in initializeForBatching: execution not a ClosingExecutionResult");
            System.exit(1);
        }
        ClosingExecutionResult cr = (ClosingExecutionResult)this.execution;

        if(!(cr.inner() instanceof StandardInternalExecutionResult)) {
            System.out.println("Error in initializeForBatching: cr inner not a StandardInternalExecutionResult");
            System.exit(1);
        }
        StandardInternalExecutionResult sr = (StandardInternalExecutionResult)cr.inner();

        if(!(sr.runtimeResult() instanceof PipeExecutionResult)) {
            System.out.println("Error in initializeForBatching: sr runtimeResult not a PipeExecutionResult");
            System.exit(1);
        }
        PipeExecutionResult pr = (PipeExecutionResult)sr.runtimeResult();
        pr.initializeInner();
    }

    public void batchWith(Result other) {

        // Confirm proper types
        if(!(this.execution instanceof ClosingExecutionResult)) {
            System.out.println("Error in batchWith: execution not a ClosingExecutionResult");
            System.exit(1);
        }
        ClosingExecutionResult cr = (ClosingExecutionResult)this.execution;

        if(!(cr.inner() instanceof StandardInternalExecutionResult)) {
            System.out.println("Error in batchWith: cr inner not a StandardInternalExecutionResult");
            System.exit(1);
        }
        StandardInternalExecutionResult sr = (StandardInternalExecutionResult)cr.inner();

        if(!(sr.runtimeResult() instanceof PipeExecutionResult)) {
            System.out.println("Error in batchWith: sr runtimeResult not a PipeExecutionResult");
            System.exit(1);
        }
        PipeExecutionResult pr = (PipeExecutionResult)sr.runtimeResult();

        // Already can assume 'other' has proper types because would've been checked in initializeForBatching
        ResultSubscriber rs = (ResultSubscriber)other;
        ClosingExecutionResult other_cr = (ClosingExecutionResult)rs.execution;
        StandardInternalExecutionResult other_sr = (StandardInternalExecutionResult)other_cr.inner();
        PipeExecutionResult other_pr = (PipeExecutionResult)other_sr.runtimeResult();

        Pipe this_root = pr.pipe();
        while(this_root instanceof PipeWithSource) {
            this_root = ((PipeWithSource) this_root).getSource();
        }

        Pipe other_root = other_pr.pipe();
        while(other_root instanceof PipeWithSource) {
            other_root = ((PipeWithSource) other_root).getSource();
        }

        if(other_root instanceof NodeByLabelScanPipe) {
            if(this_root instanceof NodeByLabelScanPipe) {
                ((NodeByLabelScanPipe)this_root).setNodes(((NodeByLabelScanPipe)other_root).nodes());
            }
            else if (this_root instanceof AllNodesScanPipe) {
                // TODO
            }
            else {
                System.out.println("Error in batchWith: unknown type of this_root");
                System.exit(1);
            }
        } else if (other_root instanceof AllNodesScanPipe) {
            // TODO
        }
        else {
            System.out.println("Error in batchWith: unknown type of other_root");
            System.exit(1);
        }
    }

    public void setUseCached(boolean useCached) {
        // Confirm proper types
        if(!(this.execution instanceof ClosingExecutionResult)) {
            System.out.println("Error in batchWith: execution not a ClosingExecutionResult");
            System.exit(1);
        }
        ClosingExecutionResult cr = (ClosingExecutionResult)this.execution;

        if(!(cr.inner() instanceof StandardInternalExecutionResult)) {
            System.out.println("Error in batchWith: cr inner not a StandardInternalExecutionResult");
            System.exit(1);
        }
        StandardInternalExecutionResult sr = (StandardInternalExecutionResult)cr.inner();

        if(!(sr.runtimeResult() instanceof PipeExecutionResult)) {
            System.out.println("Error in batchWith: sr runtimeResult not a PipeExecutionResult");
            System.exit(1);
        }
        PipeExecutionResult pr = (PipeExecutionResult)sr.runtimeResult();

        Pipe this_root = pr.pipe();
        while(this_root instanceof PipeWithSource) {
            this_root = ((PipeWithSource) this_root).getSource();
        }

        try {


            if (this_root instanceof NodeByLabelScanPipe) {
                ((LazyNodeValueCursorIterator) ((NodeByLabelScanPipe) this_root).nodes()).setUseCached(useCached);
            }
            else if (this_root instanceof AllNodesScanPipe) {
                // TODO
                //((LazyNodeValueCursorIterator)((AllNodesScanPipe)this_root).nodes()).setUseCached(useCached);
            }
            else {
                System.out.println("Error in setUseCached: unknown type of this_root");
                System.exit(1);
            }


        } catch (Exception e) {
            System.out.println("Exception in setUseCached");
        }


    }


}
