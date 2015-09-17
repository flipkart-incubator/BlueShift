package fdp.Resource;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by sushil.s
 * Date : 16/09/15
 * Time : 10:34 AM
 */
@Getter
@Setter
public class UpdateCronScheduleBean {
    private String namespace;
    private String jobName;
    private String cronExprStr;
}
