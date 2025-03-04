package com.rite.products.convertrite.configuration;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.rite.products.convertrite.po.BasicResponsePo;

@Slf4j
@Aspect
@Component
public class LoggingAspect {

    // Define pointcut for controller methods
    @Pointcut("within(com.rite.products.convertrite.controller..*)")
    public void controllerMethods() {}

    // Define pointcut to exclude certain methods
    @Pointcut("!execution(* com.rite.products.convertrite.controller.CrDataController.onScheduledJob(..))")
    public void excludeMethods() {}

    // Combine pointcuts
    @Pointcut("controllerMethods() && excludeMethods()")
    public void loggingPointcut() {}

    @Around("loggingPointcut()")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        String methodName = joinPoint.getSignature().getName();
        log.info("Start execution of {} ", methodName);
        Object proceed = joinPoint.proceed();
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000; // convert to seconds
        log.info("End execution of {} took {} seconds", methodName, duration);
        return proceed;
    }

    @Pointcut("@annotation(org.springframework.web.bind.annotation.ExceptionHandler)")
    public void exceptionHandlerMethods() {
    }

    // Advice method to execute code after an exception is thrown
    @AfterReturning(pointcut = "exceptionHandlerMethods()", returning = "ex")
    public void logError(JoinPoint joinPoint,ResponseEntity<BasicResponsePo> ex) {
        log.error("Exception in {} with cause: {}", joinPoint.getSignature().getDeclaringType(),ex.getBody().getError());
        log.error("Exception message {}",ex.getBody().getMessage());
    }
}
