package ucr.ppci.nosql.movies.model;

public class MovieModel extends BaseEntityModel {

    final public static String MOVIES_COLLECTION_NAME = "movies";

    private String originalLanguage;
    private String overview;
    private String releaseDate;
    private float budget;
    private float revenue;
    private float runtime;
    private boolean adult;

    public String getOriginalLanguage() {
        return originalLanguage;
    }

    public void setOriginalLanguage(String originalLanguage) {
        this.originalLanguage = originalLanguage;
    }

    public String getOverview() {
        return overview;
    }

    public void setOverview(String overview) {
        this.overview = overview;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public float getBudget() {
        return budget;
    }

    public void setBudget(float budget) {
        this.budget = budget;
    }

    public float getRevenue() {
        return revenue;
    }

    public void setRevenue(float revenue) {
        this.revenue = revenue;
    }

    public float getRuntime() {
        return runtime;
    }

    public void setRuntime(float runtime) {
        this.runtime = runtime;
    }

    public boolean isAdult() {
        return adult;
    }

    public void setAdult(boolean adult) {
        this.adult = adult;
    }

    @Override
    public String toString() {
        return "MovieModel{" +
                "_key=" + super.getKey() +
                ", name='" + super.getName() + '\'' +
                ", originalLanguage='" + originalLanguage + '\'' +
                ", overview='" + overview + '\'' +
                ", releaseDate='" + releaseDate + '\'' +
                ", budget=" + budget +
                ", revenue=" + revenue +
                ", runtime=" + runtime +
                ", adult=" + adult +
                '}';
    }
}
