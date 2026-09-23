package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;

/**
 * Writes a temporary file and reads it back, then checksums it. Disk-bound: CPU stays low while
 * the task is slow, which breaks naive CPU-based scheduling (spec §7.2).
 *
 * <p>{@code mode} selects the phases: {@code write} only writes, {@code read} writes the fixture
 * then reads it back, {@code both} (the default) additionally compares the checksums. The file
 * always lives under the worker's configured temp dir and is deleted afterwards, including when
 * the task is cancelled mid-write: the loops check for interrupts and the delete runs in a
 * {@code finally} block.
 */
public class FileIoTaskExecutor implements TaskExecutor {

    static final int CHUNK = 256 * 1024;

    private final Path tempDir;

    /** Uses the JVM temp dir; the worker passes its configured dir instead. */
    public FileIoTaskExecutor() {
        this(Path.of(System.getProperty("java.io.tmpdir")));
    }

    public FileIoTaskExecutor(Path tempDir) {
        this.tempDir = tempDir;
    }

    public Path tempDir() {
        return tempDir;
    }

    @Override
    public TaskType type() {
        return TaskType.FILE_IO_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.IO_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int sizeMb = Integer.parseInt(params.get("size_mb"));
        String mode = params.getOrDefault("mode", "both");
        long bytes = (long) sizeMb * 1024 * 1024;
        Path file = null;
        try {
            Files.createDirectories(tempDir);
            file = Files.createTempFile(tempDir, "predisched-", ".bin");
            byte[] chunk = new byte[CHUNK];
            new Random(42L).nextBytes(chunk);
            MessageDigest written = MessageDigest.getInstance("SHA-256");
            long done = 0;
            try (var out = Files.newOutputStream(file, StandardOpenOption.WRITE)) {
                while (done < bytes) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new CancellationException("interrupted while writing");
                    }
                    int take = (int) Math.min(chunk.length, bytes - done);
                    out.write(chunk, 0, take);
                    written.update(chunk, 0, take);
                    done += take;
                }
            }
            long read = 0;
            String checksum = HexFormat.of().formatHex(written.digest());
            if (!mode.equals("write")) {
                MessageDigest readBack = MessageDigest.getInstance("SHA-256");
                byte[] buf = new byte[CHUNK];
                try (var in = Files.newInputStream(file, StandardOpenOption.READ)) {
                    int got;
                    while ((got = in.read(buf)) != -1) {
                        if (Thread.currentThread().isInterrupted()) {
                            throw new CancellationException("interrupted while reading");
                        }
                        readBack.update(buf, 0, got);
                        read += got;
                    }
                }
                String readChecksum = HexFormat.of().formatHex(readBack.digest());
                if (mode.equals("both") && !readChecksum.equals(checksum)) {
                    return ExecutionResult.failure("checksum mismatch after read-back");
                }
                checksum = readChecksum;
            }
            return ExecutionResult.success("size_mb=" + sizeMb + " mode=" + mode
                    + " written_bytes=" + done + " read_bytes=" + read + " sha256=" + checksum);
        } catch (CancellationException e) {
            throw e;
        } catch (Exception e) {
            return ExecutionResult.failure("file I/O failed: " + e.getMessage());
        } finally {
            if (file != null) {
                try {
                    Files.deleteIfExists(file);
                } catch (Exception ignored) {
                    // Best effort: a leftover is reported by the no-temp-files test, not hidden.
                }
            }
        }
    }
}
