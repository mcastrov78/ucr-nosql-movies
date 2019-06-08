package ucr.ppci.nosql.movies.csv;

import ucr.ppci.nosql.movies.db.ArangoDBConnection;
import ucr.ppci.nosql.movies.model.BaseEdgeModel;
import ucr.ppci.nosql.movies.model.BaseEntityModel;
import ucr.ppci.nosql.movies.model.MovieModel;
import ucr.ppci.nosql.movies.model.RatingModel;
import ucr.ppci.nosql.movies.Movies;

public class RatingRowProcessor extends BaseRowProcessor {

    final private static ArangoDBConnection arangoDBConnection = ArangoDBConnection.getInstance();

    public void process(String cells[]) {
        RatingModel rating = new RatingModel();

        rating.setUserID(cells[0]);
        rating.setRating(Float.parseFloat(cells[2]));
        rating.setTimeStamp(cells[3]);

        System.out.println("RatingModel: " + rating.toString());
        arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, RatingModel.RATINGS_COLLECTION_NAME, rating);

        processRatings(cells[1], rating);

    }

    private void processRatings(String movieKey, RatingModel ratingModel) {
        try {
            String ratingID = movieKey + ratingModel.getTimeStamp();

            // add edge -> relationship
            BaseEdgeModel edge = new BaseEdgeModel();
            edge.setFrom(MovieModel.MOVIES_COLLECTION_NAME + "/" + movieKey);
            edge.setTo(BaseEntityModel.RATINGS_COLLECTION_NAME + "/" + ratingID);
            arangoDBConnection.addDocument(Movies.MOVIES_DB_NAME, BaseEdgeModel.RATINGS_EDGE_COLLECTION_NAME, edge);
        } catch (Exception ex) {
            System.err.println("Failed to process rating value. " + ex.getMessage());
        }
    }
}
