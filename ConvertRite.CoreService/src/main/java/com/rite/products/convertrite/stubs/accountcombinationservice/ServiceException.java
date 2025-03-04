/**
 * ServiceException.java
 *
 * <p>This file was auto-generated from WSDL by the Apache Axis2 version: 1.8.0 Built on : Aug 01,
 * 2021 (07:27:19 HST)
 */
package com.rite.products.convertrite.stubs.accountcombinationservice;

public class ServiceException extends java.lang.Exception {

  private static final long serialVersionUID = 1672406294245L;

  private com.rite.products.convertrite.stubs
          .accountcombinationservice.AccountCombinationServiceStub.ServiceErrorMessageE
      faultMessage;

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
      com.rite.products.convertrite.stubs
              .accountcombinationservice.AccountCombinationServiceStub.ServiceErrorMessageE
          msg) {
    faultMessage = msg;
  }

  public com.rite.products.convertrite.stubs
          .accountcombinationservice.AccountCombinationServiceStub.ServiceErrorMessageE
      getFaultMessage() {
    return faultMessage;
  }
}
