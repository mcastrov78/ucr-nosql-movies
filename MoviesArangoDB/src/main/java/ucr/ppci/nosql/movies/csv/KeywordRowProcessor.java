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
import ucr.ppci.nosql.movies.model.BaseEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieModel;

import java.util.HashSet;
import java.util.Set;

public class KeywordRowProcessor extends BaseRowProcessor {

    final private static Logger logger = LoggerFactory.getLogger(KeywordRowProcessor.class);
    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set keywordsCache = new HashSet<BaseEntityModel>();

    @Override
    public void process(String cells[]) {

        //only process complete rows
        if (cells.length >= 2) {
            // these JSON values INCORRECTLY use single quotes for string values and GSON parser can fix that
            processKeywords(cells[0], Util.sanitizeJson(cells[1]));
        } else {
            logger.warn("\tKeywords row has less fields than expected");
        }
    }

    private void processKeywords(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each item in the list
            for (Object keywordObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)keywordObject;
                objectId = jsonObject.get("id").toString();

                // check if item is not already in cache (and so in the DB)
                if (!keywordsCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add item to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.KEYWORDS_COLLECTION_NAME, model);
                    keywordsCache.add(objectId);
                }

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(BaseEntityModel.KEYWORDS_COLLECTION_NAME + "/" + objectId);
                edge.setTo(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.KEYWORDS_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            logger.error("Failed to parse JSON value ({}). {}", jsonString, pe.getMessage());
        }
    }

}
