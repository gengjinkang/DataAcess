package com.fuhao.data.fhbase.inter;

public interface Dml {

    void createTable(String tableName, String... familyColumn);

    void dropTable(String tableName);

    boolean tableExists(String tableName);

    void truncateTable(String tableName);
}
