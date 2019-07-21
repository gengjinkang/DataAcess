package com.fuhao.data.es.api;


import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;

public class EsSimpleExpression {
    private String fieldName;
    private Object value;
    private Collection<Object> values;
    private EsCriterion.Operator operator;
    private Object from;
    private Object to;

    protected EsSimpleExpression(String fieldName, Object value, EsCriterion.Operator operator){
        this.fieldName = fieldName;
        this.value= value ;
        this.operator=  operator ;
    }
    protected EsSimpleExpression(String fieldName,  EsCriterion.Operator operator){
        this.fieldName = fieldName;
        this.operator=  operator ;
    }
    protected EsSimpleExpression(String fieldName,Collection<Object> values){
        this.fieldName = fieldName;
        this.values = values;
        this.operator = EsCriterion.Operator.TERM;
    }

    protected EsSimpleExpression(String fieldName, Object from, Object to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = EsCriterion.Operator.RANGE;
    }

    public QueryBuilder toBuilder(){
        QueryBuilder qb = null;
        switch (operator) {
            case MATCH:
                qb = QueryBuilders.matchQuery(fieldName,value);
                break;
            case TERM:
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true).includeUpper(true);
                break;
            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
                break;
            case WILDCARD:
                qb = QueryBuilders.wildcardQuery(fieldName,value.toString());
                break;
        }
        return qb;
    }
}
