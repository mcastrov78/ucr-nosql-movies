package ucr.ppci.nosql.movies.model;

public class ActorMovieEdgeModel extends BaseEdgeModel {

    final public static String CREDITS_ACTOR_MOVIE_EDGE_COLLECTION_NAME = "actsIn";

    private String character;

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    @Override
    public String toString() {
        return "ActorMovieEdgeModel{" +
                "_from='" + super.getFrom() + '\'' +
                ", _to='" + super.getTo() + '\'' +
                "character='" + character + '\'' +
                '}';
    }

}
