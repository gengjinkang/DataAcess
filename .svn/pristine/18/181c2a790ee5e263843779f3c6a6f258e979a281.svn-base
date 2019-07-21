package com.fuhao.data.fhbase;

import java.io.Serializable;

/**
 * Created by root on 7/12/17.
 */
public class HBaseBean implements Serializable {

    private static final long serialVersionUID = 1L;
    private String row;
    private String family;
    private String qualifier;
    private String value;
    private long timestamp;

    public HBaseBean() {
    }

    public HBaseBean(String row, String family, String qualifier, String value, long timestamp) {
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
    }

    public HBaseBean(String row, String family, String qualifier, String value) {
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public String getFamily() {
        return this.family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getColumn() {
        return this.qualifier;
    }

    public String getQualifier() {
        return qualifier;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("{");
        sb.append("\"row\":").append("\""+row+"\",");
        sb.append("\"family\":").append("\""+family+"\",");
        sb.append("\"column\":").append("\""+qualifier+"\",");
        sb.append("\"value\":").append("\""+value+"\",");
        sb.append("\"timestamp\":").append("\""+timestamp+"\"");
        sb.append('}');
        return sb.toString();
    }

    public void setQualifier(String column) {
        this.qualifier = column;
    }

    public String getRow() {
        return this.row;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
