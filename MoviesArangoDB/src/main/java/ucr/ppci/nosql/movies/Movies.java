package ucr.ppci.nosql.movies;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import ucr.ppci.nosql.movies.csv.*;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Movies {

    final private static Logger logger = LoggerFactory.getLogger(Movies.class);
    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    final public static String MOVIES_DB_NAME = "Movies";

    private void deleteCollections() {
        // delete edge collections
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_GENRES_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COMPANIES_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COUNTRIES_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.KEYWORDS_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEdgeModel.RATINGS_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, MovieCastEdgeModel.CREDITS_CAST_EDGE_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, MovieCrewEdgeModel.CREDITS_CREW_EDGE_COLLECTION_NAME);

        // delete movies collections
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, MovieModel.MOVIES_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.GENRES_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.COMPANIES_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.SPOKEN_LANGUAGES_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.COUNTRIES_COLLECTION_NAME);

        // delete keywords collections
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.KEYWORDS_COLLECTION_NAME);

        // delete ratings collections
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, RatingModel.RATINGS_COLLECTION_NAME);

        // delete credits collections
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.CAST_COLLECTION_NAME);
        arangoDBConnection.deleteCollection(MOVIES_DB_NAME, BaseEntityModel.CREW_COLLECTION_NAME);
    }

    private void createCollections() {
        // create movies collections
        arangoDBConnection.createCollection(MOVIES_DB_NAME, MovieModel.MOVIES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.GENRES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.COMPANIES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.SPOKEN_LANGUAGES_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.COUNTRIES_COLLECTION_NAME);

        // create keywords collections
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.KEYWORDS_COLLECTION_NAME);

        // create ratings collections
        arangoDBConnection.createCollection(MOVIES_DB_NAME, RatingModel.RATINGS_COLLECTION_NAME);

        // create credits collections
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.CAST_COLLECTION_NAME);
        arangoDBConnection.createCollection(MOVIES_DB_NAME, BaseEntityModel.CREW_COLLECTION_NAME);

        // create edge collections
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_GENRES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COMPANIES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COUNTRIES_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.KEYWORDS_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, BaseEdgeModel.RATINGS_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, MovieCastEdgeModel.CREDITS_CAST_EDGE_COLLECTION_NAME);
        arangoDBConnection.createEdgeCollection(MOVIES_DB_NAME, MovieCrewEdgeModel.CREDITS_CREW_EDGE_COLLECTION_NAME);
    }

    private void readCSV(String fileName, BaseRowProcessor modelRow) {
        int lines = 0;

        try{
            // open file reader
            Path filePath = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
            FileReader filereader = new FileReader(filePath.toString());

            // create csvReader object passing file reader as a parameter
            CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();

            // read data line by line
            Iterator<String[]> iterator = csvReader.iterator();

            for (lines = 0; iterator.hasNext(); ) {
                modelRow.process(iterator.next());
                lines++;
            }
        }
        catch (Exception e) {
            logger.error("Failed to read file {}. {}", fileName, e.getMessage());
        }

        logger.info("CSV File {} was processed successfully ({} lines read)", fileName, lines);
    }

    public static void main(String args[]) {
        Movies movies = new Movies();

        if (args.length > 0) {
            if (args[0].equals("-c")) {
                movies.deleteCollections();
            }
            else {
                System.out.println("Options are: ");
                System.out.println("\t-h Help ");
                System.out.println("\t-c Clean up DB and import data");
                System.exit(0);
            }
        }

        // create DB
        arangoDBConnection.createDB(MOVIES_DB_NAME);
        movies.createCollections();

        // read abd process CSV files
        movies.readCSV("movies_metadata_100.csv", new MovieRowProcessor());
        movies.readCSV("keywords_100.csv", new KeywordRowProcessor());
        movies.readCSV("ratings_100.csv", new RatingRowProcessor());
        movies.readCSV("credits_100.csv", new CreditsRowProcessor());

        System.exit(0);
    }

}
