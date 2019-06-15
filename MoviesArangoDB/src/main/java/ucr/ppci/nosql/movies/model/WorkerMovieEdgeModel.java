package ucr.ppci.nosql.movies.model;

public class WorkerMovieEdgeModel extends BaseEdgeModel {

    final public static String CREDITS_WORKER_MOVIE_EDGE_COLLECTION_NAME = "worksIn";

    private String job;

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String toString() {
        return "WorkerMovieEdgeModel{" +
                "_from='" + super.getFrom() + '\'' +
                ", _to='" + super.getTo() + '\'' +
                "job='" + job + '\'' +
                '}';
    }
}
