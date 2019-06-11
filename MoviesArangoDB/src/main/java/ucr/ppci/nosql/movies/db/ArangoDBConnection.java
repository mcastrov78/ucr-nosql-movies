package ucr.ppci.nosql.movies.db;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoDBConnection {

    final private static Logger logger = LoggerFactory.getLogger(ArangoDBConnection.class);

    final private static ArangoDBConnection arangoDBConnection = new ArangoDBConnection();
    final private ArangoDB arangoDB = new ArangoDB.Builder().build();

    private ArangoDBConnection() {}

    public static ArangoDBConnection getInstance() {
        return arangoDBConnection;
    }

    public void createDB(String dbName) {
        try {
            arangoDB.createDatabase(dbName);
            logger.info("Database created: {}", dbName);
        } catch (final ArangoDBException ae) {
            logger.error("Failed to create database: {}. {}", dbName, ae.getMessage());
        }
    }

    public void createCollection(String dbName, String collectionName) {
        try {
            final CollectionEntity newCollection = arangoDB.db(dbName).createCollection(collectionName);
            logger.info("Collection created: {}", collectionName);
        }
        catch (final ArangoDBException ae) {
            logger.error("Failed to create collection: {}. {}", collectionName, ae.getMessage());
        }
    }

    public void deleteCollection(String dbName, String collectionName) {
        try {
            final ArangoCollection collection = arangoDB.db(dbName).collection(collectionName);
            collection.drop();
            logger.info("Collection deleted: {}", collectionName);
        }
        catch (final ArangoDBException ae) {
            logger.error("Failed to delete collection: {}. {}", collectionName, ae.getMessage());
        }
    }

    public void createEdgeCollection(String dbName, String edgeCollectionName) {
        try {
            arangoDB.db(dbName).createCollection(edgeCollectionName, new CollectionCreateOptions().type(CollectionType.EDGES));
            logger.info("Edge Collection created: {}", edgeCollectionName);
        }
        catch (final ArangoDBException ae) {
            logger.error("Failed to create edge collection: {}. {}", edgeCollectionName, ae.getMessage());
        }
    }

    public void addDocument(String dbName, String collectionName, Object document) {
        try {
            arangoDB.db(dbName).collection(collectionName).insertDocument(document);
        }
        catch (final ArangoDBException ae) {
            logger.error("Failed to insert document in collection {} ({}). {}", collectionName, document.toString(), ae.getMessage());
        }
    }

}
