package com.fuhao.data.fhbase;


import com.fuhao.data.fhbase.inter.Deletes;
import com.fuhao.data.fhbase.inter.Dml;
import com.fuhao.data.fhbase.inter.Gets;
import com.fuhao.data.fhbase.inter.Puts;

/**
 * Created by lx on 17/10/17.
 * @author lz
 */
public interface FuhaoHbase extends Puts,Deletes,Gets,Dml {


    void batchShutDown() ;

    void awaitTermination();

    void close();

}
