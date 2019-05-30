package ucr.ppci.nosql.movies.db;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;

public class ArangoDBConnection {

    final private static ArangoDBConnection arangoDBConnection = new ArangoDBConnection();
    final private ArangoDB arangoDB = new ArangoDB.Builder().build();

    private ArangoDBConnection() {}

    public static ArangoDBConnection getInstance() {
        return arangoDBConnection;
    }

    public void createDB(String dbName) {
        try {
            arangoDB.createDatabase(dbName);
            System.out.println("Database created: " + dbName);
        } catch (final ArangoDBException ae) {
            System.err.println("Failed to create database: " + dbName + "; " + ae.getMessage());
        }
    }

    public void createCollection(String dbName, String collectionName) {
        try {
            final CollectionEntity newCollection = arangoDB.db(dbName).createCollection(collectionName);
            System.out.println("Collection created: " + collectionName);
        }
        catch (final ArangoDBException ae) {
            System.err.println("Failed to create collection: " + collectionName + "; " + ae.getMessage());
        }
    }

    public void createEdgeCollection(String dbName, String edgeCollectionName) {
        try {
            final CollectionEntity newCollection = arangoDB.db(dbName).createCollection(edgeCollectionName, new CollectionCreateOptions().type(CollectionType.EDGES));
            System.out.println("Edge Collection created: " + edgeCollectionName);
        }
        catch (final ArangoDBException ae) {
            System.err.println("Failed to create edge collection: " + edgeCollectionName + "; " + ae.getMessage());
        }
    }

    public void addDocument(String dbName, String collectionName, Object document) {
        try {
            arangoDB.db(dbName).collection(collectionName).insertDocument(document);
        }
        catch (final ArangoDBException ae) {
            System.err.println("Failed to insert document. " + ae.getMessage());
        }
    }

}
