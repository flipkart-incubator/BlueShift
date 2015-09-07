package com.flipkart.fdp.HealthCheck;

import com.codahale.metrics.health.HealthCheck;

/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 1:04 PM
 */
public class AppHealthCheck extends HealthCheck {

    private final String appName;

    public AppHealthCheck(String appName){
        this.appName = appName;
    }

    @Override
    protected Result check() throws Exception {
        final String saying = String.format(appName,"TEST");
        if(!saying.contains("TEST")){
            return Result.unhealthy("Application doesn't include a name!");
        }
        return Result.healthy();
    }
}
