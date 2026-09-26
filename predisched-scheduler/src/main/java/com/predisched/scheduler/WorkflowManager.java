package com.predisched.scheduler;

import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.obs.EventLog;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowRequest;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.TaskQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task DAGs (F1, FR22). A workflow is validated as a whole (Kahn's algorithm finds cycles), every
 * task is stored, the roots are queued and the rest wait BLOCKED. When a task completes, each
 * child whose last parent that was is released to the queue, with any {@code ${parent.result}}
 * in its input replaced by that parent's result. When a task fails or is cancelled, every
 * descendant still waiting is cancelled with {@code UPSTREAM_FAILED}.
 *
 * <p>A {@code WORKFLOW_TASK} in a workflow never goes to a worker: it depends on every other task
 * of its workflow and completes the moment it is released, so its status is the workflow's.
 *
 * <p>Thread safety: the maps are concurrent, each child's count of unfinished parents is an
 * {@link AtomicInteger}, and every status change goes through the store's atomic update, so two
 * parents finishing together release a child exactly once.
 */
public class WorkflowManager {

    private static final Logger log = LoggerFactory.getLogger(WorkflowManager.class);
    private static final Pattern RESULT_REF = Pattern.compile("\\$\\{([^}]+)\\.result}");
    public static final int MAX_TASKS = 1_000;
    public static final String UPSTREAM_FAILED = "UPSTREAM_FAILED";

    private final TaskStore store;
    private final TaskQueue queue;
    private final RetryCoordinator retries;
    private final Map<String, List<String>> children = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> unfinishedParents = new ConcurrentHashMap<>();
    private final Map<String, List<String>> workflows = new ConcurrentHashMap<>();

    public WorkflowManager(TaskStore store, TaskQueue queue, RetryCoordinator retries) {
        this.store = store;
        this.queue = queue;
        this.retries = retries;
        retries.addTerminalListener(this::onTerminal);
    }

    /**
     * Topological order by Kahn's algorithm: repeatedly take a task with no unfinished parents.
     * Tasks left over at the end sit on a cycle.
     *
     * @throws IllegalArgumentException naming the tasks on or behind a cycle
     */
    public static List<String> topologicalOrder(Map<String, List<String>> dependsOn) {
        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, List<String>> edges = new LinkedHashMap<>();
        dependsOn.forEach((task, parents) -> {
            inDegree.put(task, parents.size());
            for (String parent : parents) {
                edges.computeIfAbsent(parent, key -> new ArrayList<>()).add(task);
            }
        });
        Deque<String> ready = new ArrayDeque<>();
        inDegree.forEach((task, degree) -> {
            if (degree == 0) {
                ready.add(task);
            }
        });
        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String task = ready.poll();
            order.add(task);
            for (String child : edges.getOrDefault(task, List.of())) {
                if (inDegree.merge(child, -1, Integer::sum) == 0) {
                    ready.add(child);
                }
            }
        }
        if (order.size() < dependsOn.size()) {
            List<String> stuck = new ArrayList<>(dependsOn.keySet());
            stuck.removeAll(order);
            throw new IllegalArgumentException("workflow has a cycle through " + stuck);
        }
        return order;
    }

    /** Every problem with a workflow; empty means it can be submitted. */
    public List<String> validate(WorkflowRequest request, TaskValidator validator) {
        List<String> errors = new ArrayList<>();
        String workflowId = request.getWorkflowId();
        if (workflowId.isBlank()) {
            errors.add("workflow_id must be non-empty");
        } else if (workflows.containsKey(workflowId)) {
            errors.add("workflow already exists: " + workflowId);
        }
        if (request.getTasksCount() == 0) {
            errors.add("a workflow needs at least one task");
        }
        if (request.getTasksCount() > MAX_TASKS) {
            errors.add("a workflow may hold at most " + MAX_TASKS + " tasks (got "
                    + request.getTasksCount() + ")");
        }
        Set<String> ids = new HashSet<>();
        for (TaskRequest task : request.getTasksList()) {
            if (!ids.add(task.getTaskId())) {
                errors.add("duplicate task id in workflow: " + task.getTaskId());
            }
        }
        Map<String, List<String>> dependsOn = new LinkedHashMap<>();
        for (TaskRequest task : request.getTasksList()) {
            boolean hasReference = RESULT_REF.matcher(task.getInput()).find();
            for (String error : validator.validate(task, store, !hasReference)) {
                errors.add(task.getTaskId() + ": " + error);
            }
            for (String parent : task.getDependsOnList()) {
                if (parent.equals(task.getTaskId())) {
                    errors.add(task.getTaskId() + " depends on itself");
                } else if (!ids.contains(parent)) {
                    errors.add(task.getTaskId() + " depends on unknown task " + parent);
                }
            }
            Matcher reference = RESULT_REF.matcher(task.getInput());
            while (reference.find()) {
                if (!task.getDependsOnList().contains(reference.group(1))) {
                    errors.add(task.getTaskId() + " uses the result of " + reference.group(1)
                            + ", which is not one of its depends_on");
                }
            }
            dependsOn.put(task.getTaskId(), List.copyOf(task.getDependsOnList()));
        }
        if (errors.isEmpty()) {
            try {
                topologicalOrder(dependsOn);
            } catch (IllegalArgumentException e) {
                errors.add(e.getMessage());
            }
        }
        return errors;
    }

    /**
     * Stores a validated workflow and queues its roots. Every record is stored before any root
     * is queued, so a fast root can never finish before its children exist.
     */
    public List<String> submit(WorkflowRequest request, String clientId, String traceId) {
        Map<String, List<String>> dependsOn = new LinkedHashMap<>();
        Map<String, TaskRequest> byId = new LinkedHashMap<>();
        for (TaskRequest task : request.getTasksList()) {
            dependsOn.put(task.getTaskId(), List.copyOf(task.getDependsOnList()));
            byId.put(task.getTaskId(), task);
        }
        List<String> order = topologicalOrder(dependsOn);
        String workflowId = request.getWorkflowId();
        for (String id : order) {
            List<String> parents = dependsOn.get(id);
            unfinishedParents.put(id, new AtomicInteger(parents.size()));
            for (String parent : parents) {
                children.computeIfAbsent(parent, key -> new ArrayList<>()).add(id);
            }
        }
        workflows.put(workflowId, order);
        List<String> roots = new ArrayList<>();
        for (String id : order) {
            TaskRequest task = byId.get(id);
            TaskRecord record = TaskRecord.createQueued(id, task.getType(), task.getInput(),
                            task.getPriority(), traceId, task.getTimeoutMs(),
                            task.getMaxRetries() == 0 ? -1 : task.getMaxRetries())
                    .withClientId(clientId)
                    .withWorkflowId(workflowId);
            if (dependsOn.get(id).isEmpty()) {
                roots.add(id);
                store.put(record);
            } else {
                store.put(record.asBlocked());
            }
        }
        EventLog.get().event("WORKFLOW_SUBMIT", workflowId, Map.of(
                "tasks", String.valueOf(order.size()), "roots", String.valueOf(roots.size())));
        log.info("Workflow {} accepted: {} tasks, {} ready now", workflowId, order.size(),
                roots.size());
        for (String id : roots) {
            release(id);
        }
        return order;
    }

    /** Every task of a workflow in topological order, or empty if unknown. */
    public List<TaskRecord> status(String workflowId) {
        List<TaskRecord> records = new ArrayList<>();
        for (String id : workflows.getOrDefault(workflowId, List.of())) {
            TaskRecord record = store.get(id);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    public boolean isWorkflowTask(String taskId) {
        return unfinishedParents.containsKey(taskId);
    }

    /** Terminal-state listener: release children on success, cancel them otherwise. */
    void onTerminal(TaskRecord record) {
        List<String> waiting = children.getOrDefault(record.id(), List.of());
        if (waiting.isEmpty()) {
            return;
        }
        if (record.status() == TaskStatus.COMPLETED) {
            for (String child : waiting) {
                if (unfinishedParents.get(child).decrementAndGet() == 0) {
                    release(child);
                }
            }
        } else {
            for (String child : waiting) {
                cancel(child, UPSTREAM_FAILED + ": " + record.id() + " " + record.status());
            }
        }
    }

    /** A task whose parents are all done: substitute results, then queue it (or finish it). */
    private void release(String taskId) {
        TaskRecord current = store.get(taskId);
        if (current == null) {
            return;
        }
        String input = substitute(current.input());
        if (current.status() == TaskStatus.BLOCKED) {
            List<String> errors = TaskInputSpec.validate(current.type(), input);
            if (!errors.isEmpty()) {
                cancel(taskId, "input after substitution is invalid: " + String.join("; ", errors));
                return;
            }
            boolean[] releasedHere = {false};
            TaskRecord released = store.update(taskId, record -> {
                if (record.status() != TaskStatus.BLOCKED) {
                    return record;
                }
                releasedHere[0] = true;
                return record.withInput(input).withStatus(TaskStatus.QUEUED);
            });
            if (!releasedHere[0]) {
                return;
            }
            current = released;
        }
        if (current.status() != TaskStatus.QUEUED) {
            return;
        }
        if (current.type() == TaskType.WORKFLOW_TASK) {
            // The workflow's own record: nothing to run, it is done when its parents are.
            TaskRecord done = store.update(taskId, record -> record
                    .withStatus(TaskStatus.RUNNING)
                    .withResult("workflow " + record.workflowId() + ": all tasks completed")
                    .withStatus(TaskStatus.COMPLETED));
            log.info("Workflow {} completed", done.workflowId());
            retries.notifyTerminal(done);
            return;
        }
        EventLog.get().event("WORKFLOW_RELEASE", taskId, Map.of("workflow", current.workflowId()));
        queue.add(taskId, current.priority(), current.submittedAt());
    }

    private void cancel(String taskId, String reason) {
        TaskRecord before = store.get(taskId);
        if (before == null || before.status() != TaskStatus.BLOCKED) {
            return;
        }
        boolean[] cancelledHere = {false};
        TaskRecord after = store.update(taskId, record -> {
            if (record.status() != TaskStatus.BLOCKED) {
                return record;
            }
            cancelledHere[0] = true;
            return record.withResult(reason).withStatus(TaskStatus.CANCELLED);
        });
        if (cancelledHere[0]) {
            log.info("Task {} cancelled: {}", taskId, reason);
            retries.notifyTerminal(after);
        }
    }

    /** Replaces each {@code ${taskId.result}} with that task's result. */
    String substitute(String input) {
        Matcher matcher = RESULT_REF.matcher(input);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            TaskRecord parent = store.get(matcher.group(1));
            String value = parent == null ? "" : parent.result();
            matcher.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
