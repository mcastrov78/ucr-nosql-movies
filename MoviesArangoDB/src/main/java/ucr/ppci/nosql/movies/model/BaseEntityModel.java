package ucr.ppci.nosql.movies.model;

public class BaseEntityModel {

    final public static String GENRES_COLLECTION_NAME = "genres";
    final public static String COMPANIES_COLLECTION_NAME = "companies";
    final public static String SPOKEN_LANGUAGES_COLLECTION_NAME = "spokenLanguages";
    final public static String COUNTRIES_COLLECTION_NAME = "countries";
    final public static String KEYWORDS_COLLECTION_NAME = "keywords";

    private String _key;
    private String name;

    public String getKey() {
        return _key;
    }

    public void setKey(String _key) {
        this._key = _key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "BaseEntityModel{" +
                "_key='" + _key + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
