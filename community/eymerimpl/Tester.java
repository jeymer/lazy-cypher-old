package eymerimpl;

import com.stoke.*;
import com.stoke.types.KnobValT;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

import java.io.File;
import java.util.Scanner;

public class Tester {
    static String graph_name = "/Users/Jeff/code/neo4j-inputs/graphs/user-graph";
    static String database_name = "neo4j";
    static File graph_file = new File(graph_name);

    static final String traceLocation = "/Users/Jeff/code/neo4j-inputs/traces";

    static final String operationLocation
            = traceLocation + "/operations/";
    static final String intervalLocation
            = traceLocation + "/intervals/";

    /* Supported Pipes:
     *
     * AllNodesScan
     * NodesByLabelScan
     * ProduceResults
     * Filter
     * Limit
     * Projection
     *
     * Create
     * Delete
     */

    public static void iteratorTest(Transaction tx) {
        tx.lazyExecute("MATCH (n:Person) WHERE n.id < 2 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id = 3 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id >= 1 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id = 1 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id <> 6 RETURN n;");

        tx.lazyExecute("MATCH (n:Person) WHERE n.id < 2 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id = 3 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id >= 1 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id = 1 RETURN n;");
        tx.lazyExecute("MATCH (n:Person) WHERE n.id <> 6 RETURN n;");
    }

    public static void main(String[] args) {

        GraphDatabaseService graphdb = new DatabaseManagementServiceBuilder(graph_file)
                .build().database(database_name);
        try (Transaction tx = graphdb.beginTx()) {

            // Setup Aeneas Machine
            Reward reward = new Reward(null) {
                @Override
                public double valuate() {
                    return tx.getNumCompletedInSeconds(1, System.currentTimeMillis());
                }
                @Override
                public double SLA() {
                    return 3.0;
                }
            };
            // Setup knobs
            Knob twitter_knob = new DiscreteKnob("stride-size", KnobValT.haveIntegers(417000, 834000, 1668000,3336000)); // 1%,2%,4%,8%
            Knob so_knob = new DiscreteKnob("stride-size", KnobValT.haveIntegers(118000,236000,472000,944000)); // 1%,2%,4%,8%
            Knob github_knob = new DiscreteKnob("stride-size", KnobValT.haveIntegers(44000,88000,176000,352000)); // 1%,2%,4%,8%

            AeneasMachine aeneas = new AeneasMachine(StochasticPolicyType.NO_STOCHASTIC, new Knob[]{twitter_knob}, reward);
            aeneas.start();


            long a = tx.lazyExecute("MATCH (n:User) WHERE n.id <> 6 RETURN n;");
            long b = tx.lazyExecute("MATCH (n:User) WHERE n.id = 5 RETURN n;");
            long c = tx.lazyExecute("MATCH (n:User) WHERE n.id = 6 RETURN n;");
            tx.batchDelayed();
            while(tx.operationsRemaining() > 0) {
                tx.propagateFirst();
            }
        } catch (Exception e) {
        e.printStackTrace();
        System.out.println(e.toString());
        System.exit(1);
    }




        /*
        GraphDatabaseService graphdb = new DatabaseManagementServiceBuilder(graph_file)
                .build().database(database_name);
        try (Transaction tx = graphdb.beginTx()) {

            tx.setNumThreads(2);

            Scanner operation_stream =
                    new Scanner(new File(operationLocation + "UsersUnique1000Cypher.txt"));
            Scanner interval_stream =
                    new Scanner(new File(intervalLocation + "intervals-10-1000.txt"));

            int lines_read = 0;
            int lines_to_read = 100;
            long started_at = System.currentTimeMillis();

            tx.startPropagation();

            while (operation_stream.hasNextLine() && lines_read < lines_to_read) {
                String operation = operation_stream.nextLine();
                long interval = Long.parseLong(interval_stream.nextLine());
                while (System.currentTimeMillis() - started_at < interval) {
                    tx.batchDelayed();
                }
                tx.lazyExecute(operation);
                lines_read++;
            }

            System.out.println("Operation stream finished");

            while (tx.operationsRemaining() != 0) {
                tx.batchDelayed();
            }

            tx.stopPropagation();

            System.out.println("Time taken: " + (System.currentTimeMillis() - started_at));
            System.out.println("Operation times:");
            for(int i = 0; i < lines_to_read; i++) {
                System.out.print("" + i + "\t");
                tx.getOperationTime(i);
            }

            tx.rollback();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.exit(1);
        }
         */
    }
}
