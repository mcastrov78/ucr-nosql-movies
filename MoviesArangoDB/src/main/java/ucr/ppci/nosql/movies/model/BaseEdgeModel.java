package ucr.ppci.nosql.movies.model;

public class BaseEdgeModel {

    final public static String MOVIES_GENRES_EDGE_COLLECTION_NAME = "moviesGenres";
    final public static String MOVIES_COMPANIES_EDGE_COLLECTION_NAME = "moviesCompanies";

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
}
