package ucr.ppci.nosql.movies.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucr.ppci.nosql.movies.Util;
import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.*;
import ucr.ppci.nosql.movies.Movies;

import java.util.HashSet;
import java.util.Set;

public class RatingRowProcessor extends BaseRowProcessor {

    final private static Logger logger = LoggerFactory.getLogger(MovieRowProcessor.class);
    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    private Set usersCache = new HashSet<BaseEntityModel>();

    public void process(String cells[]) {
        BaseEntityModel user = new BaseEntityModel();

        //only process complete rows
        if (cells.length >= 4) {
            user.setKey(cells[0]);

            // check if item is not already in cache (and so in the DB)
            if (!usersCache.contains(cells[0])) {
                // add user to DB and cache
                arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEntityModel.USERS_COLLECTION_NAME, user);
                usersCache.add(cells[0]);
            }

            // add edge -> relationship
            UserMovieEdgeModel edge = new UserMovieEdgeModel();
            edge.setFrom(BaseEntityModel.USERS_COLLECTION_NAME + "/" + cells[0]);
            edge.setTo(MovieModel.MOVIES_COLLECTION_NAME + "/" + cells[1]);
            edge.setRating(Util.parseFloat(cells[2]));
            edge.setTimeStamp(cells[3]);
            arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, UserMovieEdgeModel.USER_MOVIE_EDGE_COLLECTION_NAME, edge);
        } else {
            logger.warn("\t Ratings row has less fields than expected");
        }
    }

}
