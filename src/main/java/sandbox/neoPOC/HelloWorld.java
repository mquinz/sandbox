package sandbox.neoPOC;
import org.neo4j.driver.v1.*;

import java.util.Random;

import static org.neo4j.driver.v1.Values.parameters;

public class HelloWorld implements AutoCloseable
{
    private  Driver driver = null;

    private  static int JOB_SIZE = 10000000;

    private static Random generator = new Random();

    private static String getLicenseQuery = " MATCH  (a:AnonId {id:$anId})-[:HAS_FIRST]->(fa:FirstAnonId)-[:RELATED_TO]->(at:AtlassianId)-[:USER_HAS_LICENSE]->(l:License) Return l.name limit 1";

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void printGreeting( final String message )
    {

        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "12345"
;
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );

        try ( Session session = driver.session() )
        {
            runQueries(session);

//            String greeting = session.writeTransaction( new TransactionWork<String>()
//            {
//                @Override
//                public String execute( Transaction tx )
//                {
//                    StatementResult result = tx.run( "CREATE (a:Greeting) " +
//                                    "SET a.message = $message " +
//                                    "RETURN a.message + ', from node ' + id(a)",
//                            parameters( "message", message ) );
//                    return result.single().get( 0 ).asString();
//                }
//            } );
//            System.out.println( greeting );
        }
    }

    private static void runQueries(Session session) {


        Transaction tx = session.beginTransaction();


        long startTimer = System.currentTimeMillis();

        long elapsed;

        for (int x=0; x < JOB_SIZE; x++) {
         //   Integer anId =  generator.nextInt(10000) ;
            StatementResult result = tx.run(getLicenseQuery,
                    parameters("anId", generator.nextInt(100000) ));
            String licenseId = result.single().get(0).asString();
//            if (x % 100000 ==0) {
//                elapsed = System.currentTimeMillis() - startTimer;
//                System.out.println(x + " anId: " + anId + "  license " + licenseId + " elapsed= " + elapsed);
//            }
        }


        tx.close();
        session.close();
         elapsed = System.currentTimeMillis() - startTimer;

        System.out.println(" -------------  Summary Totals   -------------");

        System.out.println(" Elapsed Time:               " + elapsed + " ms");
        System.out.println(" Number of queries :                 " + JOB_SIZE );
        System.out.println(" Avg ms per query:          " + ( JOB_SIZE  / elapsed));
        System.out.println(" Avg queries per second:    " + ( JOB_SIZE  / elapsed * 1000));



    }

    public static void main( String... args ) throws Exception
    {
        try ( HelloWorld greeter = new HelloWorld(  ) )
        {
            greeter.printGreeting( "hello, world" );
        }
    }
}