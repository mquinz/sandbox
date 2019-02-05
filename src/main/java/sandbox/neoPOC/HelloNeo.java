package sandbox.neoPOC;


import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;


public class HelloNeo {


    public static void main( String[] args )
    {
        System.out.println( "Hello Neo4j!" );

        Config noSSL = Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig();
  //      Driver driver = GraphDatabase.driver("bolt://54.167.174.205:38274",AuthTokens.basic("neo4j","conspiracies-offset-mines"),noSSL); // <password>
        Driver driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j","12345"),noSSL); // <password>
        try (Session session = driver.session()) {
            String cypherQuery =
                    "MATCH (n) " +
                            "RETURN id(n) AS id limit 100";
            StatementResult result = session.run(cypherQuery, parameters());
            while (result.hasNext()) {
                System.out.println(result.next().get("id"));
            }

            session.close();
            driver.close();
        }

    }


}
