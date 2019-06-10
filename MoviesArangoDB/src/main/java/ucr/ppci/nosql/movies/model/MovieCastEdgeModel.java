package ucr.ppci.nosql.movies.model;

public class MovieCastEdgeModel extends BaseEdgeModel {

    final public static String CREDITS_CAST_EDGE_COLLECTION_NAME = "moviesCast";

    private String character;

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    @Override
    public String toString() {
        return "MovieCastEdgeModel{" +
                "_from='" + super.getFrom() + '\'' +
                ", _to='" + super.getTo() + '\'' +
                "character='" + character + '\'' +
                '}';
    }

}
