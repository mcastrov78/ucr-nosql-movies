package ucr.ppci.nosql.movies.model;

public class BaseEdgeModel {

    final public static String MOVIES_GENRES_EDGE_COLLECTION_NAME = "moviesGenres";
    final public static String MOVIES_COMPANIES_EDGE_COLLECTION_NAME = "moviesCompanies";
    final public static String MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME = "moviesSpokenLanguages";
    final public static String MOVIES_COUNTRIES_EDGE_COLLECTION_NAME = "moviesCountries";
    final public static String KEYWORDS_EDGE_COLLECTION_NAME = "moviesKeywords";
    final public static String RATINGS_EDGE_COLLECTION_NAME = "moviesRatings";

    private String _from;
    private String _to;

    public String getFrom() {
        return _from;
    }

    public void setFrom(String _from) {
        this._from = _from;
    }

    public String getTo() {
        return _to;
    }

    public void setTo(String _to) {
        this._to = _to;
    }

    @Override
    public String toString() {
        return "BaseEdgeModel{" +
                "_from='" + _from + '\'' +
                ", _to='" + _to + '\'' +
                '}';
    }
}
