package sandbox.neoPOC;

import org.apache.log4j.Logger;
import org.neo4j.driver.v1.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


public class MicroBatchLoader {

    private static Logger log = Logger.getLogger(MicroBatchLoader.class);

    private static final int TRANSACTION_LIMIT = 25;
    private int JOB_SIZE = 500;
    private int batchCtr = 0;
    private int recCtr = 0;

    private Driver driver;
    private Session session;
    private Transaction tx;




    private static final String nodeQuery = "UNWIND $props AS props " +
            "MERGE (n:Person {name:props.name}) " +
            "on create " +
            "SET n.guid = props.guid, " +
            " n.title = props.title ";


    private static final String edgeQuery = "UNWIND $props AS props " +
            "MATCH (from:Person )  where from.name = props.from " +
            "MATCH (to:Person {name:props.to}) " +
            "MERGE (from)-[:LIKES]->(to) ";



    private String[] keys = new String[JOB_SIZE];

    public static void main(String[] args) {

        log.info("begin");


        MicroBatchLoader csvImporter = new MicroBatchLoader();

        Driver driver = GraphDatabase.driver(
                "bolt://localhost:7687", AuthTokens.basic("neo4j", "12345"));
        Session session = driver.session();


        csvImporter.loadUUID(session);

        session.close();

        driver.close();


    }


    private void loadUUID(Session session) {
        this.session = session;


        long startJob = System.currentTimeMillis();
        try {


            log.info(" begin job ");
            log.info(" -------------  Job Info   -------------");
            log.info(" batch size:                  " + TRANSACTION_LIMIT );
            log.info(" job size:                    " + JOB_SIZE );
            log.info(" ---------------------------------------");



            // initialize the lists that will contain the nodes and edge properties
            List<HashMap<String, Object>> lNodeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);
            List<HashMap<String, Object>> lEdgeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);


            for (int j = 0; j < JOB_SIZE; j++) {


                /*
                 create a synthetic node.
                  Each node will be a HashMap with <k><v> pairs for the attributes
                   Each node will be added to an ArrayList that will be passed to the server in a single transaction.
                  */

                HashMap<String, Object> hmNodeProps = new HashMap();
                //    String key = j + "-" + System.currentTimeMillis();
                String key = j + "-" + j;
                hmNodeProps.put("name", key);
                hmNodeProps.put("guid", "" + UUID.randomUUID());
                lNodeProps.add(hmNodeProps);

                /*
                create a synthetic relationship
                 */

                HashMap<String, Object> hmEdgeProps = new HashMap();

                hmEdgeProps.put("from", key);
                if (recCtr > 0) {
                    int randomIdx = ThreadLocalRandom.current().nextInt(0, recCtr);
                    hmEdgeProps.put("to", keys[randomIdx]);
                    lEdgeProps.add(hmEdgeProps);
             }
                    keys[j] = key;



                // commit transaction as needed
                if (j > 0 && j % TRANSACTION_LIMIT == 0) { //
                //

                    batchCtr++;


                    long startBatch = System.currentTimeMillis();



                    tx = session.beginTransaction();
                    /*
                        call the batch loader.  pass the list of maps to the cypher engine
                        to merge the nodes, then commit the transaction and close it

                      */
                    loadBatch(nodeQuery, lNodeProps);

                    // start new transaction for the edges
                    tx = session.beginTransaction();

                    loadBatch(edgeQuery, lEdgeProps);


                    long endBatch = System.currentTimeMillis();


                    log.debug(" record " + recCtr + " batch: " + batchCtr + "  elapsed:" + (endBatch- startBatch));


                    // prepare for next batch by resetting the lists containing the batches of records
                    lNodeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);
                    lEdgeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);

                }


                this.recCtr++;


            }


            long endJob = System.currentTimeMillis();

            log.info(" -------------  Summary Totals   -------------");

            log.info(" job elapsed:                 " + (endJob - startJob) + "ms");
            log.info(" nodes created/merged:        " + recCtr );
            log.info(" relationships created/merged:" + recCtr );
            log.info(" batch size:                  " + TRANSACTION_LIMIT );
            log.info(" number of batches:           " + batchCtr );

            log.info(" average time per batch:      " + (endJob - startJob) / batchCtr + "ms");


        } catch (Throwable e) {

            log.error(" Throwable Exception " + e.getMessage());
            e.printStackTrace();
        } finally {
            tx.close();
        }

        return;

    }

/*

pass the list of hashmaps to neo as part of a single transaction per batch
 */

    private void loadBatch(String query, List<HashMap<String, Object>>  lProps) {


        long startBatch = System.currentTimeMillis();
        tx.run(query, Collections.singletonMap("props", lProps));
        long endRun = System.currentTimeMillis();
        log.debug(" batch: " + batchCtr + " run elapsed:" + (endRun - startBatch));

        tx.success();
        tx.close();

        long endBatch = System.currentTimeMillis();
        log.debug(" batch: " + batchCtr + " commit elapsed:" + (endBatch - startBatch));


    }


}