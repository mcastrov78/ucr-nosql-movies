package ucr.ppci.nosql.movies.csv;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucr.ppci.nosql.movies.Movies;
import ucr.ppci.nosql.movies.Util;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.ActorMovieEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.WorkerMovieEdgeModel;
import ucr.ppci.nosql.movies.model.MovieModel;

import java.util.HashSet;
import java.util.Set;

public class CreditsRowProcessor extends BaseRowProcessor {

    final private static Logger logger = LoggerFactory.getLogger(CreditsRowProcessor.class);
    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set actorsCache = new HashSet<BaseEntityModel>();
    private Set workersCache = new HashSet<BaseEntityModel>();

    public void process(String cells[]) {

        // these JSON values INCORRECTLY use single quotes for string values and GSON parser can fix that
        processCast(cells[2], Util.sanitizeJson(cells[0]));
        processCrew(cells[2], Util.sanitizeJson(cells[1]));
    };

    private void processCast(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object castObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)castObject;
                objectId = jsonObject.get("id").toString();

                // check if item is not already in cache (and so in the DB)
                if (!actorsCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add item to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.ACTORS_COLLECTION_NAME, model);
                    actorsCache.add(objectId);
                }

                // add edge -> relationship
                ActorMovieEdgeModel edge = new ActorMovieEdgeModel();
                edge.setFrom(BaseEntityModel.ACTORS_COLLECTION_NAME + "/" + objectId);
                edge.setTo(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setCharacter(String.valueOf(jsonObject.get("character")));
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, ActorMovieEdgeModel.CREDITS_ACTOR_MOVIE_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

    private void processCrew(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process item in the list
            for (Object crewObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)crewObject;
                objectId = jsonObject.get("id").toString();

                // check if item is not already in cache (and so in the DB)
                if (!workersCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add item to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.WORKERS_COLLECTION_NAME, model);
                    workersCache.add(objectId);
                }

                // add edge -> relationship
                WorkerMovieEdgeModel edge = new WorkerMovieEdgeModel();
                edge.setFrom(BaseEntityModel.WORKERS_COLLECTION_NAME + "/" + objectId);
                edge.setTo(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setJob(String.valueOf(jsonObject.get("job")));
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, WorkerMovieEdgeModel.CREDITS_WORKER_MOVIE_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

}
