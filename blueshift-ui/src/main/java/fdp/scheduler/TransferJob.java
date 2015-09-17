package fdp.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;

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
        String fileName = context.getJobDetail().getKey() + "log";
        String transferNativeCommand = "sh execute_blueshift " + dcmConfig + " " + fileName;
        //String transferNativeCommand = "echo \"Hello, World!The BlueShift is gonna Rock!\"";
        log.info("Executing the native command...");
        try {
            Process transferProcess = Runtime.getRuntime().exec(transferNativeCommand);

//            try {
            transferProcess.waitFor();
//                BufferedReader reader = new BufferedReader(
//                        new InputStreamReader(
//                                transferProcess.getInputStream()));
//                BufferedWriter out = new BufferedWriter(new FileWriter(new File("/tmp/bs-"+context.getJobDetail().getKey()+".log" )));
//                String line;
//                while((line = reader.readLine()) != null) {
//                    out.write(line);
//                    out.newLine();
//                }
//                out.close();
            log.info("Sucessfully Executed!");
//            } catch (InterruptedException e) {
//                BufferedReader reader = new BufferedReader(
//                        new InputStreamReader(
//                                transferProcess.getInputStream()));
//                BufferedWriter out = new BufferedWriter(new FileWriter(new File("/tmp/bs-"+String.valueOf(context.getJobDetail().getKey())+ DateTime.now().toString())+".error" ));
//                String line;
//                while((line = reader.readLine()) != null) {
//                    out.write(line);
//                    out.newLine();
//                }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
