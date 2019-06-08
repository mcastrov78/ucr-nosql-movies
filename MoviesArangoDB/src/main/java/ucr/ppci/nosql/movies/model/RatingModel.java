package ucr.ppci.nosql.movies.model;

public class RatingModel extends BaseEntityModel {

    final public static String RATINGS_COLLECTION_NAME = "ratings";

    private String userID;
    private float rating;
    private String timeStamp;

    public String getUserID() { return userID; }

    public void setUserID(String userID) { this.userID = userID; }

    public float getRating() { return rating; }

    public void setRating(float rating) { this.rating = rating; }

    public String getTimeStamp() { return timeStamp; }

    public void setTimeStamp(String timeStamp) { this.timeStamp = timeStamp; }

    @Override
    public String toString() {
        return "RatingModel{" +
                "_key=" + super.getKey() +
                ", name='" + super.getName() + '\'' +
                ", userID='" + userID + '\'' +
                ", rating='" + rating + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }

}
