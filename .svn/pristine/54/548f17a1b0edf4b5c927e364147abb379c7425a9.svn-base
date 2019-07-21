package com.fuhao.data.fhbase.inter;

import java.util.List;

public interface Deletes {

    boolean delete(String tableName, String rowKey, String family, String qualifier);

    boolean delete(String tableName, String rowKey) ;

    boolean delete(String tableName, String rowKey, String family) ;

    boolean deletes(String tableName, String family, List<String> rows, List<String> cloums);

    boolean deleteList(String tableName, String family, String rows, List<String> cloums);
}
