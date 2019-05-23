package ucr.ppci.nosql.movies;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import ucr.ppci.nosql.movies.model.*;

public class Movies {

    final private static ArangoDB arangoDB = new ArangoDB.Builder().build();
    final private static String MOVIES_DB_NAME = "Movies";
    final private static String MOVIES_COLLECTION_NAME = "movies";

    private void createDB(ArangoDB arangoDB, String dbName) {
        try {
            arangoDB.createDatabase(dbName);
            System.out.println("Database created: " + dbName);
        } catch (final ArangoDBException ae) {
            System.err.println("Failed to create database: " + dbName + "; " + ae.getMessage());
        }
    }

    private void createCollection(ArangoDB arangoDB, String dbName, String collectionName) {
        try {
            final CollectionEntity newCollection = arangoDB.db(dbName).createCollection(collectionName);
            System.out.println("Collection created: " + collectionName);
        }
        catch (final ArangoDBException ae) {
            System.err.println("Failed to create collection: " + collectionName + "; " + ae.getMessage());
        }
    }

    private void addDocument(ArangoDB arangoDB, String dbName, String collectionName, BaseDocument document) {
        try {
            arangoDB.db(dbName).collection(collectionName).insertDocument(document);
            System.out.println("Document inserted");
        }
        catch (final ArangoDBException ae) {
            System.err.println("Failed to insert document. " + ae.getMessage());
        }
    }

    private Movie mapToMovie(String cells[]) {
        Movie movie = new Movie();

        movie.setTitle(cells[20]);

        return movie;
    };

    private void readCSV() {
        try{
            // open file reader
            Path filePath = Paths.get(getClass().getClassLoader().getResource("movies_metadata_100.csv").toURI());
            System.out.println("CVS PATH: " + filePath.toString());
            FileReader filereader = new FileReader(filePath.toString());

            // create csvReader object passing file reader as a parameter
            CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();
            String[] nextRecord;

            // read data line by line
            while ((nextRecord = csvReader.readNext()) != null) {
                Movie thisMovie = mapToMovie(nextRecord);
                System.out.println("Movie: " + thisMovie.getTitle());
            }
        }
        catch (Exception e) {
            System.err.println("Failed to read file. " + e.getMessage());
        }
    }

    public static void main(String args[]) {
        Movies movies = new Movies();

        //final ArangoDB arangoDB = new ArangoDB.Builder().build();

        // create DB
        movies.createDB(arangoDB, MOVIES_DB_NAME);

        // create collection
        movies.createCollection(arangoDB, MOVIES_DB_NAME, MOVIES_COLLECTION_NAME);

        // read file
        movies.readCSV();

        // add document
        BaseDocument thisMovie = new BaseDocument();
        thisMovie.setKey("1");
        thisMovie.addAttribute("name", "Avengers");

        movies.addDocument(arangoDB, MOVIES_DB_NAME, MOVIES_COLLECTION_NAME, thisMovie);
    }

}
