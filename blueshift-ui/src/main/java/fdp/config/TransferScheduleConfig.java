package fdp.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import fdp.scheduler.TransferJob;
import lombok.Getter;
import lombok.Setter;
import org.quartz.*;

/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 1:26 AM
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransferScheduleConfig {
    private final String transferGroupName;
    private final String transferJobName;
    private final String cronScheduleStr;
    private final String dcmConfig;


    public TransferScheduleConfig(String transferGroupName,
                                  String transferJobName,
                                  String cronScheduleStr,
                                  String dcmConfig) {
        this.transferGroupName = transferGroupName;
        this.transferJobName   = transferJobName;
        this.cronScheduleStr   = cronScheduleStr;
        this.dcmConfig         = dcmConfig;
    }

    private JobDetail jobDetail;
    private JobKey jobKey;
    private Trigger trigger;

    public void initialize() {
        this.jobKey = new JobKey(getTransferJobName(), getTransferGroupName());
        jobDetail   = JobBuilder.newJob(TransferJob.class)
                     .withIdentity(jobKey)
                     .build();
        CronScheduleBuilder cronSchedule = CronScheduleBuilder
                                           .cronSchedule(getCronScheduleStr());
        trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(getTransferJobName(), getTransferGroupName())
                .usingJobData("dcmConfig", dcmConfig)
                .withSchedule(cronSchedule)
                .build();
    }

}
