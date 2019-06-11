package ucr.ppci.nosql.movies.csv;

import com.google.gson.JsonParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucr.ppci.nosql.movies.Movies;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.BaseEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieModel;

import java.util.HashSet;
import java.util.Set;

public class MovieRowProcessor extends BaseRowProcessor {

    final private static Logger logger = LoggerFactory.getLogger(MovieRowProcessor.class);
    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set genresCache = new HashSet<BaseEntityModel>();
    private Set companiesCache = new HashSet<BaseEntityModel>();
    private Set spokenLanguagesCache = new HashSet<BaseEntityModel>();
    private Set countriesCache = new HashSet<BaseEntityModel>();

    public void process(String cells[]) {
        MovieModel movie = new MovieModel();

        movie.setKey(cells[5]);
        movie.setName(cells[20]);
        movie.setOriginalLanguage(cells[7]);
        movie.setOverview(cells[9]);
        movie.setReleaseDate(cells[14]);
        movie.setBudget(Float.parseFloat(cells[2]));
        movie.setRevenue(Float.parseFloat(cells[15]));
        movie.setRuntime(Float.parseFloat(cells[16]));
        movie.setAdult(Boolean.parseBoolean(cells[0]));

        // add movie to DB
        arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, MovieModel.MOVIES_COLLECTION_NAME, movie);

        // these JSON values INCORRECTLY use single quotes for string values and GSON parser can fix that
        String sanitizedJson = new JsonParser().parse(cells[3]).toString();
        processGenres(movie.getKey(), sanitizedJson);

        sanitizedJson = new JsonParser().parse(cells[12]).toString();
        processCompanies(movie.getKey(), sanitizedJson);

        sanitizedJson = new JsonParser().parse(cells[13]).toString();
        processCountries(movie.getKey(), sanitizedJson);

        sanitizedJson = new JsonParser().parse(cells[17]).toString();
        processSpokenLanguages(movie.getKey(), sanitizedJson);

        /*
        processGenres(movie.getKey(), cells[3].replace("'", "\""));
        processCompanies(movie.getKey(), cells[12].replace("'", "\""));
        processCountries(movie.getKey(), cells[13].replace("'", "\""));
        processSpokenLanguages(movie.getKey(), cells[17].replace("'", "\""));
        */
    };

    private void processGenres(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object genreObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)genreObject;
                objectId = jsonObject.get("id").toString();

                // check if item is not already in cache (and so in the DB)
                if (!genresCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add item to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.GENRES_COLLECTION_NAME, model);
                    genresCache.add(objectId);
                }

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setTo(BaseEntityModel.GENRES_COLLECTION_NAME + "/" + objectId);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.MOVIES_GENRES_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

    private void processCompanies(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object thisObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)thisObject;
                objectId = jsonObject.get("id").toString();

                // check if item is not already in cache (and so in the DB)
                if (!companiesCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add genre to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.COMPANIES_COLLECTION_NAME, model);
                    companiesCache.add(objectId);
                }

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setTo(BaseEntityModel.COMPANIES_COLLECTION_NAME + "/" + objectId);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COMPANIES_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

    private void processSpokenLanguages(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object spokenLanguageObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)spokenLanguageObject;
                objectId = jsonObject.get("iso_639_1").toString();

                // check if item is not already in cache (and so in the DB)
                if (!spokenLanguagesCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add item to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.SPOKEN_LANGUAGES_COLLECTION_NAME, model);
                    spokenLanguagesCache.add(objectId);
                }

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setTo(BaseEntityModel.SPOKEN_LANGUAGES_COLLECTION_NAME + "/" + objectId);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

    private void processCountries(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object countryObject : jsonArray.toArray()) {
                jsonObject = (JSONObject) countryObject;
                objectId = jsonObject.get("iso_3166_1").toString();

                // check if item is not already in cache (and so in the DB)
                if (!countriesCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add production_country to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.COUNTRIES_COLLECTION_NAME, model);
                    countriesCache.add(objectId);
                }

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setTo(BaseEntityModel.COUNTRIES_COLLECTION_NAME + "/" + objectId);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.MOVIES_COUNTRIES_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

}
