package com.fuhao.data.es.api;


import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import java.util.ArrayList;
import java.util.List;

public class EsQueryBuilderConstructor{
    int size = Integer.MAX_VALUE;

    private int from = 0;

    private String asc;

    private String desc;

    //查询条件容器
    private List<EsCriterion> mustCriterions = new ArrayList<>();
    private List<EsCriterion> shouldCriterions = new ArrayList<>();
    private List<EsCriterion> mustNotCriterions = new ArrayList<>();
    private List<EsCriterion> filterCriterions = new ArrayList<>();


    public QueryBuilder listBuilders(){
        int count = mustCriterions.size()+shouldCriterions.size()+mustNotCriterions.size();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        QueryBuilder queryBuilder = null;

        if(count>=1){
           if(!isEmpty(mustCriterions)){
               for (EsCriterion criterion:mustCriterions){
                   for(QueryBuilder builder:criterion.listBuilders()){
                        queryBuilder = boolQueryBuilder.must(builder);
                   }
               }
           }
            if(!isEmpty(shouldCriterions)){
                for (EsCriterion criterion:shouldCriterions){
                    for(QueryBuilder builder:criterion.listBuilders()){
                        queryBuilder = boolQueryBuilder.should(builder);
                    }
                }
            }
            if(!isEmpty(mustNotCriterions)){
                for (EsCriterion criterion:mustNotCriterions){
                    for(QueryBuilder builder:criterion.listBuilders()){
                        queryBuilder = boolQueryBuilder.mustNot(builder);
                    }
                }
            }
            if(!isEmpty(filterCriterions)){
                for (EsCriterion criterion:filterCriterions){
                    for(QueryBuilder builder:criterion.listBuilders()){
                        queryBuilder = boolQueryBuilder.filter(builder);
                    }
                }
            }
            return queryBuilder;
        }else{
            return null;
        }
    }

    public EsQueryBuilderConstructor must(EsCriterion esCriterion){
        if(esCriterion!=null){
            mustCriterions.add(esCriterion);
        }
        return this;
    }
    public EsQueryBuilderConstructor should(EsCriterion esCriterion){
        if(esCriterion!=null){
            shouldCriterions.add(esCriterion);
        }
        return this;
    }
    public EsQueryBuilderConstructor mustNot(EsCriterion esCriterion){
        if(esCriterion!=null){
            mustNotCriterions.add(esCriterion);
        }
        return this;
    }

    public EsQueryBuilderConstructor filter(EsCriterion esCriterion){
        if(esCriterion!=null){
            filterCriterions.add(esCriterion);
        }
        return this;
    }
    public int getSize(){
        return size;
    }

    public void setSize(int size){
        this.size = size;
    }
    public String getAsc() {
        return asc;
    }

    public void setAsc(String asc) {
        this.asc = asc;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public boolean isEmpty(List lists){
        return lists==null|| lists.size()==0;
    }
}
