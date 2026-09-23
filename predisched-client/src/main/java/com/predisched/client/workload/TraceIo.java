package com.predisched.client.workload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes trace files: one JSON object per line, UTF-8, LF line endings. Writing is
 * byte-deterministic for the same entries, which is what the same-seed test asserts (rule 8).
 */
public final class TraceIo {

    private TraceIo() {}

    public static void writeTrace(Path path, List<TraceEntry> entries) throws IOException {
        StringBuilder out = new StringBuilder();
        for (TraceEntry entry : entries) {
            out.append(entry.toJson()).append('\n');
        }
        Files.writeString(path, out.toString(), StandardCharsets.UTF_8);
    }

    public static List<TraceEntry> readTrace(Path path) throws IOException {
        List<TraceEntry> entries = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                entries.add(TraceEntry.fromJson(line));
            }
        }
        return entries;
    }
}
