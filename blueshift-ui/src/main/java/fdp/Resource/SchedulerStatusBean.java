package fdp.Resource;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * Created by sushil.s
 * Date : 13/09/15
 * Time : 12:06 PM
 */
@Getter
@Setter
@AllArgsConstructor
public class SchedulerStatusBean {
    private String namespace;
    private String jobName;
    private String scheduleCronExp;
    private Date nextFireTime;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
