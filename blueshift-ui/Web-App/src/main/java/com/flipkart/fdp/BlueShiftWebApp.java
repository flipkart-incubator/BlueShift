package com.flipkart.fdp;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 3:20 PM
 */
public class BlueShiftWebApp {
    public static void main(String[] args) throws Exception {
        // The simple Jetty config here will serve static content from the webapp directory
        String webappDirLocation = "src/main/WEB/";

        String webPort = System.getenv("PORT");
        if (webPort == null || webPort.isEmpty()) {
            webPort = "8080";
        }
        Server server = new Server(Integer.valueOf(webPort));

        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setDescriptor(webappDirLocation + "/WEB-INF/web.xml");
        webapp.setResourceBase(webappDirLocation);

        server.setHandler(webapp);
        server.start();
        server.join();
    }
}
