package com.flipkart.fdp.migration.distcp.config;

import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;

/**
 * Created by sushil.s on 31/08/15.
 */
public class InputFileMapConfig {
    HashMap<String, MirrorDCMImpl.FileTuple> inputFileMap;

    public HashMap<String, MirrorDCMImpl.FileTuple> getInputFileMap() {
        return inputFileMap;
    }

    public void setInputFileMap(HashMap<String, MirrorDCMImpl.FileTuple> inputFileMap) {
        this.inputFileMap = inputFileMap;
    }
}
