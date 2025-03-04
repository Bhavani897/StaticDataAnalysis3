/**
 * HCMDataLoaderCallbackHandler.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.7.9  Built on : Nov 16, 2018 (12:05:37 GMT)
 */
package com.rite.products.convertrite.hcm.stubs;


/**
 *  HCMDataLoaderCallbackHandler Callback class, Users can extend this class and implement
 *  their own receiveResult and receiveError methods.
 */
public abstract class HCMDataLoaderCallbackHandler {
    protected Object clientData;

    /**
     * User can pass in any object that needs to be accessed once the NonBlocking
     * Web service call is finished and appropriate method of this CallBack is called.
     * @param clientData Object mechanism by which the user can pass in user data
     * that will be avilable at the time this callback is called.
     */
    public HCMDataLoaderCallbackHandler(Object clientData) {
        this.clientData = clientData;
    }

    /**
     * Please use this constructor if you don't want to set any clientData
     */
    public HCMDataLoaderCallbackHandler() {
        this.clientData = null;
    }

    /**
     * Get the client data
     */
    public Object getClientData() {
        return clientData;
    }

    /**
     * auto generated Axis2 call back method for getEntityList method
     * override this method for handling normal response from getEntityList operation
     */
    public void receiveResultgetEntityList(
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetEntityListResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getEntityList operation
     */
    public void receiveErrorgetEntityList(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getDataSetStatus method
     * override this method for handling normal response from getDataSetStatus operation
     */
    public void receiveResultgetDataSetStatus(
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetDataSetStatusResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDataSetStatus operation
     */
    public void receiveErrorgetDataSetStatus(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for importAndLoadData method
     * override this method for handling normal response from importAndLoadData operation
     */
    public void receiveResultimportAndLoadData(
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ImportAndLoadDataResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from importAndLoadData operation
     */
    public void receiveErrorimportAndLoadData(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getDfltObjAttrHints method
     * override this method for handling normal response from getDfltObjAttrHints operation
     */
    public void receiveResultgetDfltObjAttrHints(
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetDfltObjAttrHintsResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDfltObjAttrHints operation
     */
    public void receiveErrorgetDfltObjAttrHints(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getServiceLastUpdateTime method
     * override this method for handling normal response from getServiceLastUpdateTime operation
     */
    public void receiveResultgetServiceLastUpdateTime(
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetServiceLastUpdateTimeResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getServiceLastUpdateTime operation
     */
    public void receiveErrorgetServiceLastUpdateTime(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out
}
