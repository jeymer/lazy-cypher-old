package org.neo4j.procedure.impl;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ProcedureExampleTest
{
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GraphDatabaseService db;

    @Test
    public void listSuperNodesShouldWork() throws Throwable
    {
        // Given
        new JarBuilder().createJarFor( plugins.newFile( "myProcedures.jar" ), ProcedureExample.class );
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath() )
                .newGraphDatabase();

        try ( Transaction ignore = db.beginTx() )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

            node1.createRelationshipTo( node1, RelationshipType.withName( "KNOWS" ) );
            node1.createRelationshipTo( node2, RelationshipType.withName( "KNOWS" ) );
            node1.createRelationshipTo( node3, RelationshipType.withName( "KNOWS" ) );

            // When
            Result res = db.execute( "CALL org.neo4j.procedure.impl.findSuperNodes(2)" );

            // Then
            assertEquals( map( "degree", 3l, "nodeId", node1.getId() ), res.next() );
            assertFalse( res.hasNext() );
        }

    }

    @After
    public void tearDown()
    {
        if ( this.db != null )
        {
            this.db.shutdown();
        }
    }
}