package fdp.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by sushil.s
 * Date : 06/09/15
 * Time : 1:10 PM
 */
@Getter
@Setter
@NoArgsConstructor
public class TransferConfig extends Configuration {
    @JsonProperty
    private String appName;
}
