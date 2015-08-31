package com.flipkart.fdp.migration.distcp.config;

import com.flipkart.fdp.optimizer.api.IInputJob;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Set;

/**
 * Created by sushil.s on 31/08/15.
 */
public class OptimizedWorkloadConfig {
    List<Set<IInputJob>> splitTasks;

    public List<Set<IInputJob>> getSplitTasks() {
        return splitTasks;
    }

    public void setSplitTasks(List<Set<IInputJob>> splitTasks) {
        this.splitTasks = splitTasks;
    }
}
