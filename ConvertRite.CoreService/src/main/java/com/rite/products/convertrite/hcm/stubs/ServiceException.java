/**
 * ServiceException.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.7.9  Built on : Nov 16, 2018 (12:05:37 GMT)
 */
package com.rite.products.convertrite.hcm.stubs;

public class ServiceException extends java.lang.Exception {
    private static final long serialVersionUID = 1621331918001L;
    private com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ServiceErrorMessageE faultMessage;

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
        com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ServiceErrorMessageE msg) {
        faultMessage = msg;
    }

    public com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ServiceErrorMessageE getFaultMessage() {
        return faultMessage;
    }
}
