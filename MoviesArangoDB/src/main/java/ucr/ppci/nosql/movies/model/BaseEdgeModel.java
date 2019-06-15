package ucr.ppci.nosql.movies.model;

public class BaseEdgeModel {

    final public static String MOVIES_GENRES_EDGE_COLLECTION_NAME = "isOfGenre";
    final public static String MOVIES_COMPANIES_EDGE_COLLECTION_NAME = "producedBy";
    final public static String MOVIES_SPOKEN_LANGUAGES_EDGE_COLLECTION_NAME = "spokenIn";
    final public static String MOVIES_COUNTRIES_EDGE_COLLECTION_NAME = "producedIn";
    final public static String KEYWORDS_EDGE_COLLECTION_NAME = "describes";

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
