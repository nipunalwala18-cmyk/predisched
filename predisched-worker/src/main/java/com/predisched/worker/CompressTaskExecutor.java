package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compresses and decompresses a generated payload with gzip, then verifies the round trip.
 * A CPU and memory mix where the {@code level} knob changes the cost curve (spec §7.2).
 *
 * <p>The payload repeats a seeded 64 KiB random block, so it is compressible yet deterministic:
 * the same input always yields the same sizes and checksum.
 */
public class CompressTaskExecutor implements TaskExecutor {

    static final int BLOCK = 64 * 1024;

    @Override
    public TaskType type() {
        return TaskType.COMPRESS_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.MEMORY_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int sizeMb = Integer.parseInt(params.get("size_mb"));
        int level = params.containsKey("level") ? Integer.parseInt(params.get("level")) : 6;
        long rawBytes = (long) sizeMb * 1024 * 1024;
        try {
            byte[] block = new byte[BLOCK];
            new Random(42L).nextBytes(block);
            // The payload is the block repeated, so checksumming the stream while writing gives
            // the expected digest without holding size_mb megabytes in memory.
            MessageDigest expected = MessageDigest.getInstance("SHA-256");
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new LevelGzip(compressed, level);
            long written = 0;
            while (written < rawBytes) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new CancellationException("interrupted while compressing");
                }
                int take = (int) Math.min(block.length, rawBytes - written);
                gzip.write(block, 0, take);
                expected.update(block, 0, take);
                written += take;
            }
            gzip.close();
            byte[] packed = compressed.toByteArray();
            MessageDigest actual = MessageDigest.getInstance("SHA-256");
            long restored = 0;
            byte[] buf = new byte[BLOCK];
            try (GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(packed))) {
                int read;
                while ((read = gunzip.read(buf)) != -1) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new CancellationException("interrupted while decompressing");
                    }
                    actual.update(buf, 0, read);
                    restored += read;
                }
            }
            if (restored != rawBytes) {
                return ExecutionResult.failure(
                        "round trip lost bytes: wrote " + rawBytes + ", got " + restored);
            }
            String checksum = HexFormat.of().formatHex(actual.digest());
            if (!checksum.equals(HexFormat.of().formatHex(expected.digest()))) {
                return ExecutionResult.failure("round trip corrupted the payload");
            }
            double ratio = (double) packed.length / rawBytes;
            return ExecutionResult.success(String.format(Locale.ROOT,
                    "size_mb=%d level=%d raw_bytes=%d compressed_bytes=%d ratio=%.4f sha256=%s",
                    sizeMb, level, rawBytes, packed.length, ratio, checksum));
        } catch (CancellationException e) {
            throw e;
        } catch (Exception e) {
            return ExecutionResult.failure("compression failed: " + e.getMessage());
        }
    }

    /**
     * {@link GZIPOutputStream} takes no compression level, but its deflater is a protected field,
     * so the level is set right after construction.
     */
    private static final class LevelGzip extends GZIPOutputStream {
        LevelGzip(ByteArrayOutputStream out, int level) throws java.io.IOException {
            super(out);
            def.setLevel(level);
        }
    }
}
