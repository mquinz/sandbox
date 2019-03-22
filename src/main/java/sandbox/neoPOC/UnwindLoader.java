package sandbox.neoPOC;

//import org.apache.log4j.Logger;

import com.google.gson.Gson;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.TransientException;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class UnwindLoader {

    //   private static Logger log = Logger.getLogger(UnwindLoader.class);

    private static int TRANSACTION_LIMIT = 30000;
    private static int JOB_SIZE = 1000000;
    private static int BATCH_MAX_MILLIS = 20;

    // determines the number of times to attempt a commit retry and how long to wait between attempts=.

    private static final int[] BACKOFF = {100, 200, 500, 1000, 5000, 10000};


    private int batchCtr = 0;
    private int recCtr = 0;
    private Transaction tx;

    private static Random generator = new Random();
    private static Gson gson = new Gson();
      /*
    these query statements use a dual-batch method of ingesting data.  This method was tested and
    dropped due to better performance from a different approach.

This Cypher query uses an array of maps as a parameter.  Each item in the array
contains the data necessary to create AnonymousId nodes and the FirstAnonymousId nodes.
 */

    private static final String nodeQuery = "UNWIND $props AS props " +
            " MERGE (n:AnonId {id:props.id, timestamp:props.timestamp}) " +
            " ON CREATE SET n.opsgenie_user_id = props.userId " +
            " MERGE (:FirstAnonId {id:props.id}) ";


    private static final String edgeQuery = "UNWIND $props AS props " +
            " MATCH (an:AnonId  {id:props.id, timestamp:props.timestamp}) " +
            " MATCH (fa:FirstAnonId {id:props.id}) " +
            " MATCH (at:AtlassianId {id:props.atlasId}) " +
            " MERGE (an)-[:RELATED_TO]->(fa) " +
            " MERGE (fa)-[:RELATED_TO]->(at) " +
            "";

    /*
    // for testing in the Neo4j browser

:param batch => [{id:12345654321, timestamp:1234, userId:'XYZ', atlasId:27}, {id:666777555444, userId:'ABC',timestamp:4567, atlasId:940} ]

UNWIND $batch AS props
MERGE (n:AnonId {id:props.id, timestamp:props.timestamp})
    ON CREATE SET n.opsgenie_user_id = props.userId
MERGE (:FirstAnonId {id:props.id})
MERGE (from)-[:RELATED_TO]->(to)

UNWIND $batch AS props
MATCH (an:AnonId  {id:props.id, timestamp:props.timestamp})
MATCH (fa:FirstAnonId {id:props.id})
MATCH (at:AtlassianId {id:props.atlasId})
MERGE (an)-[:RELATED_TO]->(fa)
MERGE (fa)-[:RELATED_TO]->(at)




// an alternate approach
     */

    //
    private static final String caseQuery =
            " UNWIND $props AS props " +
                    " MATCH (at:AtlassianId {id:props.atlasId}) " +
                    "OPTIONAL MATCH (fa:FirstAnonId)-[:RELATED_TO]->(at) " +
                    "WITH props, at AS at, fa " +
                    "MERGE (an:AnonId {id:props.id}) " +
                    "WITH at,an,fa, props " +
                    "CALL apoc.do.when(fa is null, " +
                    "'CREATE (fa:FirstAnonId {id:id}) " +
                    " CREATE (an)-[:HAS_FIRST]->(fa)" +
                    " CREATE (fa)-[:RELATED_TO]->(at) " +
                    " RETURN fa as node   ' " +
                    ",   ' MERGE (an)-[:HAS_FIRST]->(node)  " +
                    " RETURN node' " +
                    ",    {id:props.id, at:at, an:an, node:fa}) YIELD value as fafa  " +
                    "RETURN at,an" +
                    "";


    private static void usage() {
        System.out.println("java -cp jarName sandbox.neoPOC.UnwindLoader maxJobRecords maxBatchDuration  maxBatchRecords ");

    }

    public static void main(String[] args) {


        if (args == null) {
            usage();
            System.exit(0);
        }

        if (args.length != 3) {
            usage();
            System.exit(0);
        }

        for (String arg : args) {
            System.out.println("begin with arg " + arg);
        }

        JOB_SIZE = Integer.parseInt(args[0]);
        BATCH_MAX_MILLIS = Integer.parseInt(args[1]);
        TRANSACTION_LIMIT = Integer.parseInt(args[2]);


//      create a neo configuration that overrides the transaction timeout default
        Config config = Config.builder()
                .withMaxTransactionRetryTime(5, TimeUnit.SECONDS)
                .build();

        Driver driver = GraphDatabase.driver(
                "bolt://localhost:7687", AuthTokens.basic("neo4j", "12345"), config);

        UnwindLoader loader = new UnwindLoader();

        Session session = driver.session();

        loader.loadTimedBatches(session);

        session.close();

        driver.close();


    }

/*
This is to simulate a stream reader/writer.  Instead of getting records from Kafka or some other streaming source,
it merely generates random nodes and relationships.

instead of generating these objects in single-item transactions, it collects the properties for the nodes
 and relationships into maps and each map is loaded into an arraylist of maps.

 At periodic intervals, the arraylists are sent to Cypher as parameters.  Cypher's unwind command will process
 each map individually.   These intervals are based either on elapsed time or record counts.

 as a matter of efficiency within Neo's transaction processors,  the batches are separated into two types:  One is
 for handling create/merge of nodes and the other is for creating relationships.  The reason for two batches is to
 avoid the eagerness behavior of the transaction engine.




 */

    private void loadTimedBatches(Session session) {
//        this.session = session;
//


        try {

            System.out.println(" begin job ");

            System.out.println(" caseQuery " + caseQuery);

            System.out.println(" -------------  Job Info   -------------");
            System.out.println(" max batch time interval -ms  " + BATCH_MAX_MILLIS);
            System.out.println(" max batch size:              " + TRANSACTION_LIMIT);
            System.out.println(" job size:                    " + JOB_SIZE);
            System.out.println(" ------------------------- -------------");


            /*
             initialize the lists that will contain the nodes and edge properties.
             With little effort, a single list of maps with properties could be used
              */


            List<HashMap<String, Object>> lNodeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);

            long startTimer = System.currentTimeMillis();

            long startJob = System.currentTimeMillis();

            int[] backoffCtr = new int[BACKOFF.length];

            for (int j = 0; j < JOB_SIZE; j++) {
                /*
                 create a synthetic node.
                  Each node will be a HashMap with <k><v> pairs for the attributes
                  Each map will be added to an ArrayList that will be passed to the server in a single transaction.
                  */

                HashMap<String, Object> hmNodeProps = new HashMap();
                Integer nodeId = generator.nextInt(10000);
                long timeStamp = System.currentTimeMillis();
                long atlasId = generator.nextInt(10000);
                hmNodeProps.put("id", nodeId);
                hmNodeProps.put("userId", "" + UUID.randomUUID());
                hmNodeProps.put("timestamp", timeStamp);
                hmNodeProps.put("atlasId", atlasId);
                lNodeProps.add(hmNodeProps);

//                if (j > 0 && j % TRANSACTION_LIMIT == 0) {
//                    System.out.println( "nodeId " + nodeId + " atlasId " + atlasId);
//                }

                long batchElapsed = System.currentTimeMillis() - startTimer;


                // commit transaction as needed
                if ((batchElapsed > BATCH_MAX_MILLIS) |
                        (j > 0 && j % TRANSACTION_LIMIT == 0)) { //
                    batchCtr++;

                    /*
                        call the batch loader.  pass the list of maps to the cypher engine
                        to merge the nodes, then commit the transaction and close it

                      */
                    tx = session.beginTransaction();
                    tx.run(caseQuery, Collections.singletonMap("props", lNodeProps));
                    tx.success();  //

                    for (int i = 0; i < BACKOFF.length; i++) {
                        try {
                            tx.close();
                            // get out of the retry loop if no exception thrown
                            break;

                        } catch (Throwable ex) {

                            if ((ex instanceof TransientException)) {
                                backoffCtr[i]++;
                            }
                            /*
                             Wait so that we don't immediately get into the same deadlock.
                             Use an increasing time duration for retries
                              */
                            try {
                                System.out.println(" sleep nbr " + i + " for  " + BACKOFF[i]);
                                Thread.sleep(BACKOFF[i]);
                            } catch (InterruptedException e) {
                                System.out.println(" sleep interrupted ");
                                throw new Exception("Interrupted", e);
                            }
                        }

                    }


                    if (batchCtr % 10 == 0) {
                        System.out.println(" record nbr: " + recCtr + " batch nbr: " + batchCtr + "  batch elapsed:"
                                + batchElapsed + " total job elapsed:" + (System.currentTimeMillis() - startJob));
                    }

                    // prepare for next batch by resetting the lists containing the batches of records
                    lNodeProps = new ArrayList<HashMap<String, Object>>(TRANSACTION_LIMIT);

                    startTimer = System.currentTimeMillis();

                }


                this.recCtr++;


            }


            long endJob = System.currentTimeMillis();

            System.out.println(" -------------  Summary Totals   -------------");

            System.out.println(" job elapsed:                         " + (endJob - startJob) + " ms");
            System.out.println(" AnonId nodes created/merged:         " + recCtr);
            System.out.println(" FirstAnonId nodes created/merged:    " + recCtr);

            System.out.println(" relationships created/merged:  " +
                    "      " + (recCtr * 2));
            System.out.println(" max batch size:                      " + TRANSACTION_LIMIT);
            System.out.println(" max batch time in ms:                " + BATCH_MAX_MILLIS);

            System.out.println(" number of batches:                   " + batchCtr);
            System.out.println(" average time per batch:              " + (endJob - startJob) / batchCtr + "ms");
            System.out.println(" average records per batch:           " + (recCtr / batchCtr));

            for (int x = 0; x < backoffCtr.length; x++) {
                System.out.println(" backoffCtr [" + BACKOFF[x] + "ms]:  " + backoffCtr[x]);
            }


        } catch (Throwable e) {

            System.out.println(" Throwable Exception " + e.getMessage());
            e.printStackTrace();


        } finally {
            tx.close();
        }

        return;

    }


}