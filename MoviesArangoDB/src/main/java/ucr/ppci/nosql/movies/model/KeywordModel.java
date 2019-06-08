package ucr.ppci.nosql.movies.model;

public class KeywordModel extends BaseEntityModel {

    final public static String KEYWORDS_COLLECTION_NAME = "keywords";

    @Override
    public String toString() {
        return "KeywordModel{" +
                "_key=" + super.getKey() +
                ", name='" + super.getName() +
                '}';
    }
}
