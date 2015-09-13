package fdp.application;


import fdp.HealthCheck.AppHealthCheck;
import fdp.Resource.SchedulerResource;
import fdp.config.TransferConfig;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 12:56 PM
 */

public class BlueShiftApp extends Application<TransferConfig>{

    public static void main(String[] args) throws Exception {
        new BlueShiftApp().run(args);
    }

    @Override
    public void initialize(Bootstrap<TransferConfig> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.addBundle(new AssetsBundle("/WEB","/","index.html","myStaticPages"));
    }

    @Override
    public void run(TransferConfig transferConfig, Environment environment) throws Exception {
        environment.jersey().register(new SchedulerResource());
        final AppHealthCheck healthCheck =
                new AppHealthCheck(transferConfig.getAppName());
        environment.healthChecks().register("appName",healthCheck);
    }
}
