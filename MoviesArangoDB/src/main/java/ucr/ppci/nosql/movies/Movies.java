package ucr.ppci.nosql.movies;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import ucr.ppci.nosql.movies.csv.BaseRowProcessor;
import ucr.ppci.nosql.movies.csv.MovieRowProcessor;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.BaseEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieModel;

public class Movies {

    final public static String MOVIES_DB_NAME = "Movies";

    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private void readCSV(String fileName, BaseRowProcessor modelRow) {
        try{
            // open file reader
            Path filePath = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
            FileReader filereader = new FileReader(filePath.toString());

            // create csvReader object passing file reader as a parameter
            CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();
            String[] nextRecord;

            // read data line by line
            while ((nextRecord = csvReader.readNext()) != null) {
                modelRow.process(nextRecord);
            }
        }
        catch (Exception e) {
            System.err.println("Failed to read file. " + e.getMessage());
        }
        System.out.println("<<<<<<<<<<End>>>>>>>>>>");
        System.exit(0);
    }

    public static void main(String args[]) {
        Movies movies = new Movies();

        // create DB
        arangoDBConnection.createDB(MOVIES_DB_NAME);

        // create collections
        arangoDBConnection.createCollection(MOVIES_DB_NAME, MovieModel.MOVIES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.GENRES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.COMPANIES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.SPOKEN_LANGUAGES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.COUNTRIES_COLLECTION_NAME);

        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_GENRES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COMPANIES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COUNTRIES_EDGE_COLLECTION_NAME);

        // read file
        movies.readCSV("movies_metadata_100.csv", new MovieRowProcessor());
    }

}
