package ucr.ppci.nosql.movies.csv;

import com.google.gson.JsonParser;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ucr.ppci.nosql.movies.Movies;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.MovieCastEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieCrewEdgeModel;
import ucr.ppci.nosql.movies.model.MovieModel;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CreditsRowProcessor extends BaseRowProcessor {

    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set castCache = new HashSet<BaseEntityModel>();
    private Set crewCache = new HashSet<BaseEntityModel>();

    public void process(String cells[]) {

        // this JSON values INCORRECTLY use single quotes for string values and gson parser can fix that
        String sanitizedJson = new JsonParser().parse(cells[0]).toString();
        System.out.println("Cast: " + sanitizedJson);
        processCast(cells[2], sanitizedJson);

        // this JSON values INCORRECTLY use single quotes for string values and gson parser can fix that
        sanitizedJson = new JsonParser().parse(cells[1]).toString();
        System.out.println("Crew: " + sanitizedJson);
        processCrew(cells[2], sanitizedJson);
    };

    private void processCast(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON genres values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each cast member in the list
            for (Object castObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)castObject;
                objectId = jsonObject.get("id").toString();

                // check if cast member is not already in cache (and so in the DB)
                if (!castCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add genre to DB and cache
                    //System.out.println("CAST: " + model.toString());
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.CAST_COLLECTION_NAME, model);
                    castCache.add(objectId);
                }

                // add edge -> relationship
                MovieCastEdgeModel movieCastEdgeModel = new MovieCastEdgeModel();
                movieCastEdgeModel.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                movieCastEdgeModel.setTo(BaseEntityModel.CAST_COLLECTION_NAME + "/" + objectId);
                movieCastEdgeModel.setCharacter(jsonObject.get("character").toString());
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, MovieCastEdgeModel.CREDITS_CAST_EDGE_COLLECTION_NAME, movieCastEdgeModel);
            }
        } catch (ParseException pe) {
            System.err.println("Failed to parse JSON value. " + pe.getMessage());
        }
    }

    private void processCrew(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON genres values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each crew member in the list
            for (Object crewObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)crewObject;
                objectId = jsonObject.get("id").toString();

                // check if crew member is not already in cache (and so in the DB)
                if (!crewCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add genre to DB and cache
                   // System.out.println("CREW: " + model.toString());
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.CREW_COLLECTION_NAME, model);
                    crewCache.add(objectId);
                }

                // add edge -> relationship
                MovieCrewEdgeModel movieCrewEdgeModel = new MovieCrewEdgeModel();
                movieCrewEdgeModel.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                movieCrewEdgeModel.setTo(BaseEntityModel.CREW_COLLECTION_NAME + "/" + objectId);
                movieCrewEdgeModel.setJob(jsonObject.get("job").toString());
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, MovieCrewEdgeModel.CREDITS_CREW_EDGE_COLLECTION_NAME, movieCrewEdgeModel);
            }
        } catch (ParseException pe) {
            System.err.println("Failed to parse JSON value. " + pe.toString());
        }
    }

}
