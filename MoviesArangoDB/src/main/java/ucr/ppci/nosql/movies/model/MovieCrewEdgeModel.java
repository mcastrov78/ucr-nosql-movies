package ucr.ppci.nosql.movies.model;

public class MovieCrewEdgeModel extends BaseEdgeModel {

    final public static String CREDITS_CREW_EDGE_COLLECTION_NAME = "moviesCrew";

    private String job;

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String toString() {
        return "MovieCrewEdgeModel{" +
                "_from='" + super.getFrom() + '\'' +
                ", _to='" + super.getTo() + '\'' +
                "job='" + job + '\'' +
                '}';
    }
}
