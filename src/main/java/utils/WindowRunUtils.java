package utils;

public class WindowRunUtils {
    private int numTotalQueries;
    private int numTotalCacheHit;
    private int numTotalIndividualQueries;


    public WindowRunUtils() {
        this.numTotalQueries = 0;
        this.numTotalCacheHit = 0;
        this.numTotalIndividualQueries = 0;
    }

    public void incrementNumberOfQueriesByOne() {
        this.numTotalQueries += 1;
    }

    public void incrementNumberTotalCacheHitByOne() {
        this.numTotalCacheHit += 1;
    }

    public void incrementNumberTotalIndividualQueriesByOne() {
        this.numTotalIndividualQueries += 1;
    }

    public void incrementNumberOfQueries(int numOfQueries) {
        this.numTotalQueries += numOfQueries;
    }

    public void incrementNumberOfCacheHit(int numOfCacheHit) {
        this.numTotalCacheHit += numOfCacheHit;
    }

    public int getNumTotalQueries() {
        return this.numTotalQueries;
    }

    public int getNumTotalCacheHit() {
        return this.numTotalCacheHit;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"cache_hits\": \"%d\", \"total_queries\": \"%d\"}",
                this.getNumTotalCacheHit(),
                this.getNumTotalQueries()
        );
    }
}
