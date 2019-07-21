package com.fuhao.data.es.api;

        import org.elasticsearch.index.query.QueryBuilder;

        import java.util.List;

public interface EsCriterion {
    enum Operator {
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING, MISSING,MATCH,WILDCARD
    }

    enum MatchMode {
        START, END, ANYWHERE
    }

    enum Projection {
        MAX, MIN, AVG, LENGTH, SUM, COUNT
    }

    List<QueryBuilder> listBuilders();
}