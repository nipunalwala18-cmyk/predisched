package com.predisched.common;

import com.predisched.proto.TaskRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates task submissions (FR2). Returns every error found, not just the first one.
 *
 * <ul>
 *   <li>Task ID: unique, non-empty</li>
 *   <li>Type: supported</li>
 *   <li>Input: well-formed, within size limits (from config)</li>
 *   <li>Priority: 1-10</li>
 * </ul>
 */
public class TaskValidator {

    private final int maxInputChars;

    public TaskValidator(int maxInputChars) {
        this.maxInputChars = maxInputChars;
    }

    public int maxInputChars() {
        return maxInputChars;
    }

    public List<String> validate(TaskRequest request, TaskStore store) {
        List<String> errors = new ArrayList<>();
        if (request == null) {
            errors.add("request must not be null");
            return errors;
        }
        String id = request.getTaskId();
        if (id == null || id.trim().isEmpty()) {
            errors.add("task_id must be non-empty");
        } else if (store != null && store.contains(id)) {
            errors.add("task_id already exists: " + id);
        }
        com.predisched.proto.TaskType type = request.getType();
        if (type == null || type == com.predisched.proto.TaskType.UNRECOGNIZED) {
            errors.add("type must be a supported TaskType");
        } else if (!TaskInputSpec.isKnown(type)) {
            String reason = TaskInputSpec.reservedReason(type);
            errors.add(reason == null
                    ? "type " + type + " has no executor yet; supported types are "
                            + TaskInputSpec.knownTypes()
                    : "type " + type + " is not executable yet (" + reason
                            + "); supported types are " + TaskInputSpec.knownTypes());
        }
        String input = request.getInput();
        if (input == null || input.isEmpty()) {
            errors.add("input must be non-empty");
        } else {
            if (input.length() > maxInputChars) {
                errors.add(
                        "input exceeds size limit of " + maxInputChars + " chars (got "
                                + input.length() + ")");
            }
            errors.addAll(TaskInputSpec.validate(type, input));
        }
        int priority = request.getPriority();
        if (priority < 1 || priority > 10) {
            errors.add("priority must be between 1 and 10 (got " + priority + ")");
        }
        return errors;
    }
}
