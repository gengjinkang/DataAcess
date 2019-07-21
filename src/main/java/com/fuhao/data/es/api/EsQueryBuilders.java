package com.fuhao.data.es.api;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EsQueryBuilders implements EsCriterion {

    private List<QueryBuilder> list = new ArrayList<>();


    public EsQueryBuilders match(String field,Object value){
        list.add(new EsSimpleExpression(field,value,Operator.MATCH).toBuilder());
        return this;
    }

    public EsQueryBuilders term(String field,Object value){
        list.add(new EsSimpleExpression(field,value,Operator.TERM).toBuilder());
        return this;
    }

    public EsQueryBuilders terms(String field, Collection<Object>values){
        list.add(new EsSimpleExpression(field,values).toBuilder());
        return this;
    }

    public EsQueryBuilders fuzzy(String field,Object value){
        list.add(new EsSimpleExpression(field,value,Operator.FUZZY).toBuilder());
        return this;
    }

    public EsQueryBuilders range(String field,Object from,Object to){
        list.add(new EsSimpleExpression(field,from,to).toBuilder());
        return this;
    }

    public EsQueryBuilders queryString(String queryString){
        list.add(new EsSimpleExpression(queryString,Operator.QUERY_STRING).toBuilder());
        return this;
    }
    public EsQueryBuilders wildcard(String field,String value){
        list.add(new EsSimpleExpression(field,value,Operator.WILDCARD).toBuilder());
        return this;
    }
    @Override
    public List<QueryBuilder> listBuilders() {
        return list;
    }
}
