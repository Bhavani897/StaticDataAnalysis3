/**
 * ServiceException.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.7.9  Built on : Nov 16, 2018 (12:05:37 GMT)
 */
package com.rite.products.convertrite.stubs.erp;

public class ServiceException extends java.lang.Exception {
    private static final long serialVersionUID = 1612505774624L;
    private com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ServiceErrorMessageE faultMessage;

    public ServiceException() {
        super("ServiceException");
    }

    public ServiceException(java.lang.String s) {
        super(s);
    }

    public ServiceException(java.lang.String s, java.lang.Throwable ex) {
        super(s, ex);
    }

    public ServiceException(java.lang.Throwable cause) {
        super(cause);
    }

    public void setFaultMessage(
    		com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ServiceErrorMessageE msg) {
        faultMessage = msg;
    }

    public com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ServiceErrorMessageE getFaultMessage() {
        return faultMessage;
    }
}
