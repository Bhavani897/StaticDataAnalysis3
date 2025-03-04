package com.rite.products.convertrite.utils;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
@Data
public class LookUpSets {
    private List<String> list=new ArrayList<String>();
    public LookUpSets(){
        list.add("Load Source Metadata") ;
        list.add("Define Source Template") ;
        list.add("Baseline Source Template") ;
        list.add("Load Cloud Metadata") ;
        list.add("Define Cloud Template") ;
        list.add("Identify Cloud Master Data Sets") ;
        list.add("Define Cloud Master Data") ;
        list.add("Identify Cloud Mapping Sets") ;
        list.add("Define Cloud Mapping Sets") ;
        list.add("Identify Cloud Formula Sets") ;
        list.add("Define Cloud Formula Sets") ;
        list.add("Baseline Cloud Template") ;
        list.add("Transform Data") ;
        list.add("Load to Cloud") ;
        list.add("Reconcile Data") ;
    }

}
