package com.flipkart.fdp.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.*;

/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 1:12 AM
 */
@Slf4j
public class TransferJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Executng job " + context.getJobDetail().getKey().getName());
        JobDataMap mergedJobDataMap = context.getMergedJobDataMap();
        String dcmConfig = mergedJobDataMap.getString("dcmConfig");
        //String transferNativeCommand = "hadoop jar blueshift-core-2.0.1-SNAPSHOT.jar -J" + dcmConfig;
        String transferNativeCommand = "echo \"Hello, World!The BlueShift is gonna Rock!\"";
        Process transferProcess;
        log.info("Executing the native command...");
        try {
             transferProcess = Runtime.getRuntime().exec(transferNativeCommand);

            try {

                transferProcess.waitFor();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                transferProcess.getInputStream()));
                BufferedWriter out = new BufferedWriter(new FileWriter(new File("/tmp/bs-"+context.getJobDetail().getKey()+".log" )));
                String line;
                while((line = reader.readLine()) != null) {
                    out.write(line);
                    out.newLine();
                }
                out.close();
                log.info("Sucessfully Executed!");
            } catch (InterruptedException e) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                transferProcess.getInputStream()));
                BufferedWriter out = new BufferedWriter(new FileWriter(new File("/tmp/bs-"+String.valueOf(context.getJobDetail().getKey())+ DateTime.now().toString())+".error" ));
                String line;
                while((line = reader.readLine()) != null) {
                    out.write(line);
                    out.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
