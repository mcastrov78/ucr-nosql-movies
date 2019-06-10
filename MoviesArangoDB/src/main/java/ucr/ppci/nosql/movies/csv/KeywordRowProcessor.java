package ucr.ppci.nosql.movies.csv;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ucr.ppci.nosql.movies.Movies;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.BaseEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieModel;

import java.util.HashSet;
import java.util.Set;

public class KeywordRowProcessor extends BaseRowProcessor {

    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set keywordsCache = new HashSet<BaseEntityModel>();

    @Override
    public void process(String cells[]) {

        // replace single quotes in JSON for double quotes
        System.out.println("\tKeywords: " + cells[1]);
        processKeywords(cells[0], cells[1].replace("'", "\""));
    }

    private void processKeywords(String movieKey, String jsonString) {
        JSONParser jsonParser = new JSONParser();
        BaseEntityModel model = new BaseEntityModel();

        try {
            // parse JSON keywords values
            JSONArray jsonArray = (JSONArray) jsonParser.parse(jsonString);
            JSONObject jsonObject = null;
            String objectId = null;

            // process each keyword in the list
            for (Object keywordObject: jsonArray.toArray()) {
                jsonObject = (JSONObject)keywordObject;
                objectId = jsonObject.get("id").toString();

                // check if element is not already in cache (and so in the DB)
                if (!keywordsCache.contains(objectId)) {
                    model.setKey(objectId);
                    model.setName(jsonObject.get("name").toString());

                    // add keyword to DB and cache
                    arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.KEYWORDS_COLLECTION_NAME, model);
                    keywordsCache.add(objectId);
                }

                System.out.println("KEYWORD: " + model.toString());

                // add edge -> relationship
                BaseEdgeModel edge = new BaseEdgeModel();
                edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
                edge.setTo(BaseEntityModel.KEYWORDS_COLLECTION_NAME + "/" + objectId);
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.KEYWORDS_EDGE_COLLECTION_NAME, edge);
            }
        } catch (ParseException pe) {
            System.err.println("Failed to parse JSON value. " + pe.getMessage());
        }
    }

}
