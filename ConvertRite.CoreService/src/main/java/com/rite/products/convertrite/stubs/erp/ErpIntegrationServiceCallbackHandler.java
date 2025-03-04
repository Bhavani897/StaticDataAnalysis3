/**
 * ErpIntegrationServiceCallbackHandler.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.7.9  Built on : Nov 16, 2018 (12:05:37 GMT)
 */
package com.rite.products.convertrite.stubs.erp;


/**
 *  ErpIntegrationServiceCallbackHandler Callback class, Users can extend this class and implement
 *  their own receiveResult and receiveError methods.
 */
public abstract class ErpIntegrationServiceCallbackHandler {
    protected Object clientData;

    /**
     * User can pass in any object that needs to be accessed once the NonBlocking
     * Web service call is finished and appropriate method of this CallBack is called.
     * @param clientData Object mechanism by which the user can pass in user data
     * that will be avilable at the time this callback is called.
     */
    public ErpIntegrationServiceCallbackHandler(Object clientData) {
        this.clientData = clientData;
    }

    /**
     * Please use this constructor if you don't want to set any clientData
     */
    public ErpIntegrationServiceCallbackHandler() {
        this.clientData = null;
    }

    /**
     * Get the client data
     */
    public Object getClientData() {
        return clientData;
    }

    /**
     * auto generated Axis2 call back method for uploadFileToUcm method
     * override this method for handling normal response from uploadFileToUcm operation
     */
    public void receiveResultuploadFileToUcm(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.UploadFileToUcmResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from uploadFileToUcm operation
     */
    public void receiveErroruploadFileToUcm(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for appendFileComment method
     * override this method for handling normal response from appendFileComment operation
     */
    public void receiveResultappendFileComment(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.AppendFileCommentResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from appendFileComment operation
     */
    public void receiveErrorappendFileComment(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for downloadESSJobExecutionDetails method
     * override this method for handling normal response from downloadESSJobExecutionDetails operation
     */
    public void receiveResultdownloadESSJobExecutionDetails(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.DownloadESSJobExecutionDetailsResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from downloadESSJobExecutionDetails operation
     */
    public void receiveErrordownloadESSJobExecutionDetails(
        java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for getServiceLastUpdateTime method
     * override this method for handling normal response from getServiceLastUpdateTime operation
     */
    public void receiveResultgetServiceLastUpdateTime(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetServiceLastUpdateTimeResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getServiceLastUpdateTime operation
     */
    public void receiveErrorgetServiceLastUpdateTime(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for getDocumentForDocumentId method
     * override this method for handling normal response from getDocumentForDocumentId operation
     */
    public void receiveResultgetDocumentForDocumentId(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetDocumentForDocumentIdResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDocumentForDocumentId operation
     */
    public void receiveErrorgetDocumentForDocumentId(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for importBulkData method
     * override this method for handling normal response from importBulkData operation
     */
    public void receiveResultimportBulkData(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ImportBulkDataResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from importBulkData operation
     */
    public void receiveErrorimportBulkData(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getDocumentIdsForFilePrefix method
     * override this method for handling normal response from getDocumentIdsForFilePrefix operation
     */
    public void receiveResultgetDocumentIdsForFilePrefix(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetDocumentIdsForFilePrefixResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDocumentIdsForFilePrefix operation
     */
    public void receiveErrorgetDocumentIdsForFilePrefix(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for submitJobWithOutput method
     * override this method for handling normal response from submitJobWithOutput operation
     */
    public void receiveResultsubmitJobWithOutput(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.SubmitJobWithOutputResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from submitJobWithOutput operation
     */
    public void receiveErrorsubmitJobWithOutput(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for extractAndPurge method
     * override this method for handling normal response from extractAndPurge operation
     */
    public void receiveResultextractAndPurge(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ExtractAndPurgeResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from extractAndPurge operation
     */
    public void receiveErrorextractAndPurge(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for downloadExportOutput method
     * override this method for handling normal response from downloadExportOutput operation
     */
    public void receiveResultdownloadExportOutput(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.DownloadExportOutputResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from downloadExportOutput operation
     */
    public void receiveErrordownloadExportOutput(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getEntityList method
     * override this method for handling normal response from getEntityList operation
     */
    public void receiveResultgetEntityList(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetEntityListResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getEntityList operation
     */
    public void receiveErrorgetEntityList(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for getESSExecutionDetails method
     * override this method for handling normal response from getESSExecutionDetails operation
     */
    public void receiveResultgetESSExecutionDetails(
    		com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetESSExecutionDetailsResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getESSExecutionDetails operation
     */
    public void receiveErrorgetESSExecutionDetails(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for loadAndImportData method
     * override this method for handling normal response from loadAndImportData operation
     */
    public void receiveResultloadAndImportData(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.LoadAndImportDataResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from loadAndImportData operation
     */
    public void receiveErrorloadAndImportData(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for submitESSJobRequest method
     * override this method for handling normal response from submitESSJobRequest operation
     */
    public void receiveResultsubmitESSJobRequest(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.SubmitESSJobRequestResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from submitESSJobRequest operation
     */
    public void receiveErrorsubmitESSJobRequest(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for getESSJobStatus method
     * override this method for handling normal response from getESSJobStatus operation
     */
    public void receiveResultgetESSJobStatus(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetESSJobStatusResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getESSJobStatus operation
     */
    public void receiveErrorgetESSJobStatus(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for getDocumentsForFilePrefix method
     * override this method for handling normal response from getDocumentsForFilePrefix operation
     */
    public void receiveResultgetDocumentsForFilePrefix(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetDocumentsForFilePrefixResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDocumentsForFilePrefix operation
     */
    public void receiveErrorgetDocumentsForFilePrefix(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for getDfltObjAttrHints method
     * override this method for handling normal response from getDfltObjAttrHints operation
     */
    public void receiveResultgetDfltObjAttrHints(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetDfltObjAttrHintsResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from getDfltObjAttrHints operation
     */
    public void receiveErrorgetDfltObjAttrHints(java.lang.Exception e) {
    }

    /**
     * auto generated Axis2 call back method for confirmExtractConsumption method
     * override this method for handling normal response from confirmExtractConsumption operation
     */
    public void receiveResultconfirmExtractConsumption(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ConfirmExtractConsumptionResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from confirmExtractConsumption operation
     */
    public void receiveErrorconfirmExtractConsumption(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for exportBulkData method
     * override this method for handling normal response from exportBulkData operation
     */
    public void receiveResultexportBulkData(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ExportBulkDataResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from exportBulkData operation
     */
    public void receiveErrorexportBulkData(java.lang.Exception e) {
    }

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    // No methods generated for meps other than in-out

    /**
     * auto generated Axis2 call back method for updateInterfaceData method
     * override this method for handling normal response from updateInterfaceData operation
     */
    public void receiveResultupdateInterfaceData(
       com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.UpdateInterfaceDataResponse result) {
    }

    /**
     * auto generated Axis2 Error handler
     * override this method for handling error response from updateInterfaceData operation
     */
    public void receiveErrorupdateInterfaceData(java.lang.Exception e) {
    }
}
