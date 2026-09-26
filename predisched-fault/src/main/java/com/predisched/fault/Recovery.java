package com.predisched.fault;

import com.predisched.common.TaskRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * The replicated task state sorted by what a new primary must do with each task: queue it, wait
 * on its parents, resolve it with its worker, or just remember it.
 */
public record Recovery(
        List<TaskRecord> queued,
        List<TaskRecord> blocked,
        List<TaskRecord> running,
        List<TaskRecord> finished) {

    public static Recovery of(List<TaskRecord> records) {
        List<TaskRecord> queued = new ArrayList<>();
        List<TaskRecord> blocked = new ArrayList<>();
        List<TaskRecord> running = new ArrayList<>();
        List<TaskRecord> finished = new ArrayList<>();
        for (TaskRecord record : records) {
            switch (record.status()) {
                case QUEUED -> queued.add(record);
                case BLOCKED -> blocked.add(record);
                case RUNNING -> running.add(record);
                default -> finished.add(record);
            }
        }
        return new Recovery(List.copyOf(queued), List.copyOf(blocked), List.copyOf(running),
                List.copyOf(finished));
    }

    /** Every record, whatever its state. */
    public List<TaskRecord> all() {
        List<TaskRecord> all = new ArrayList<>(queued);
        all.addAll(blocked);
        all.addAll(running);
        all.addAll(finished);
        return all;
    }

    @Override
    public String toString() {
        return queued.size() + " QUEUED, " + blocked.size() + " BLOCKED, " + running.size()
                + " RUNNING in doubt, " + finished.size() + " finished";
    }
}
