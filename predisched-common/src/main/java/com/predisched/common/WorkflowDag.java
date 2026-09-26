package com.predisched.common;

import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowRequest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.Yaml;

/**
 * A workflow file (F1): a JSON list of tasks, each with a local id and the ids it depends on.
 *
 * <pre>
 * {"tasks": [
 *   {"id": "map-1", "type": "CPU_TASK", "input": "n=3000000", "priority": 5},
 *   {"id": "reduce", "type": "SORT_TASK", "input": "n=200000", "depends_on": ["map-1"]}
 * ]}
 * </pre>
 *
 * Ids are local to the file, so one file can run many times: {@link #toRequest} prefixes every
 * id, every {@code depends_on} and every {@code ${id.result}} reference with the workflow id.
 * Parsed with SnakeYAML, which reads JSON as YAML's flow style.
 */
public final class WorkflowDag {

    /** One task in the file, ids as written. */
    public record Node(String id, TaskType type, String input, int priority,
            List<String> dependsOn) {}

    private static final Pattern RESULT_REF = Pattern.compile("\\$\\{([^}]+)\\.result}");

    private final List<Node> nodes;

    private WorkflowDag(List<Node> nodes) {
        this.nodes = List.copyOf(nodes);
    }

    public List<Node> nodes() {
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public static WorkflowDag load(Path file) throws IOException {
        Object parsed;
        try (InputStream in = Files.newInputStream(file)) {
            parsed = new Yaml().load(in);
        }
        if (!(parsed instanceof Map<?, ?> root) || !(root.get("tasks") instanceof List<?> tasks)) {
            throw new IllegalArgumentException(file + ": expected {\"tasks\": [...]}");
        }
        List<Node> nodes = new ArrayList<>();
        for (Object item : tasks) {
            if (!(item instanceof Map<?, ?> task)) {
                throw new IllegalArgumentException(file + ": every task must be an object");
            }
            Object id = task.get("id");
            Object type = task.get("type");
            if (id == null || type == null) {
                throw new IllegalArgumentException(file + ": every task needs an id and a type");
            }
            List<String> dependsOn = new ArrayList<>();
            if (task.get("depends_on") instanceof List<?> parents) {
                parents.forEach(parent -> dependsOn.add(String.valueOf(parent)));
            }
            Object priority = task.get("priority");
            nodes.add(new Node(String.valueOf(id), TaskType.valueOf(String.valueOf(type)),
                    task.get("input") == null ? "" : String.valueOf(task.get("input")),
                    priority == null ? 5 : Integer.parseInt(String.valueOf(priority)),
                    dependsOn));
        }
        return new WorkflowDag(nodes);
    }

    /** The workflow as sent to the scheduler, every local id prefixed with {@code workflowId.}. */
    public WorkflowRequest toRequest(String workflowId, String traceId) {
        WorkflowRequest.Builder request = WorkflowRequest.newBuilder().setWorkflowId(workflowId);
        for (Node node : nodes) {
            TaskRequest.Builder task = TaskRequest.newBuilder()
                    .setTaskId(qualify(workflowId, node.id()))
                    .setType(node.type())
                    .setInput(qualifyReferences(workflowId, node.input()))
                    .setPriority(node.priority())
                    .setTraceId(traceId);
            node.dependsOn().forEach(parent -> task.addDependsOn(qualify(workflowId, parent)));
            request.addTasks(task);
        }
        return request.build();
    }

    public static String qualify(String workflowId, String localId) {
        return workflowId + "." + localId;
    }

    private static String qualifyReferences(String workflowId, String input) {
        Matcher matcher = RESULT_REF.matcher(input);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(out, Matcher.quoteReplacement(
                    "${" + qualify(workflowId, matcher.group(1)) + ".result}"));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
