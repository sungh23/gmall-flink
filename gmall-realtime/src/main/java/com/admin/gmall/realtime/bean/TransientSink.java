package com.admin.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author sungh
 */
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
