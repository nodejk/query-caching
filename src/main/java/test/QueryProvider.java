package test;

import enums.Mode;
import enums.QueryType;

import java.io.IOException;
import java.util.List;

public class QueryProvider {

    public List<List<String>> queries;

    public QueryProvider(QueryType type) {
        try {
            queries = QueryReader.getQueries(10, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
