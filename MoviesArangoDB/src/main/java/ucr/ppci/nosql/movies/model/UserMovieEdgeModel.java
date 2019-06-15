package ucr.ppci.nosql.movies.model;

public class UserMovieEdgeModel extends BaseEdgeModel {

    final public static String USER_MOVIE_EDGE_COLLECTION_NAME = "rates";

    private float rating;
    private String timeStamp;

    public float getRating() { return rating; }

    public void setRating(float rating) { this.rating = rating; }

    public String getTimeStamp() { return timeStamp; }

    public void setTimeStamp(String timeStamp) { this.timeStamp = timeStamp; }

    @Override
    public String toString() {
        return "UserMovieEdgeModel{" +
                "_from='" + super.getFrom() + '\'' +
                ", _to='" + super.getTo() + '\'' +
                ", rating='" + rating + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }

}
