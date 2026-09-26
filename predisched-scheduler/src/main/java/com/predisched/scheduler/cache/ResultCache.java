package com.predisched.scheduler.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.predisched.common.InputParser;
import com.predisched.common.db.HistorySink;
import com.predisched.proto.TaskType;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Results of deterministic tasks, reused for identical work (F6, FR27, prompt 11).
 *
 * <p>The key is SHA-256 of the task type and its <em>normalised</em> input: parameters parsed,
 * trimmed and sorted by name, so {@code "n=5, seed=1"} and {@code "seed=1,n=5"} are the same
 * work. Only a result the worker marked cacheable is stored: its executor is deterministic, which
 * {@code SLEEP_TASK}, {@code HTTP_TASK}, {@code FILE_IO_TASK} and {@code DB_QUERY_TASK} are not.
 *
 * <p>Caffeine holds the hot entries in memory in front of the {@code result_cache} table. A miss
 * in memory reads the table once (the one synchronous database call, on the submit path, never
 * on the dispatch path; if the database does not answer, it is a miss). New entries and hit
 * counts go to the table asynchronously through the {@link HistorySink}. Without a database the
 * cache is memory only.
 */
public class ResultCache {

    private static final Logger log = LoggerFactory.getLogger(ResultCache.class);

    /** The worker id, and strategy, recorded on a task served from the cache. */
    public static final String WORKER = "cache";

    private final Cache<String, String> memory;
    private final DataSource database;
    private final HistorySink history;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong stored = new AtomicLong();

    /** @param database null for a memory-only cache */
    public ResultCache(long maxEntries, DataSource database, HistorySink history) {
        this.memory = Caffeine.newBuilder().maximumSize(maxEntries).build();
        this.database = database;
        this.history = history;
    }

    /** SHA-256 hex of the type and the input with its parameters sorted. */
    public static String key(TaskType type, String input) {
        StringBuilder normalised = new StringBuilder(type.name()).append('|');
        try {
            Map<String, String> params = new TreeMap<>(InputParser.parse(input));
            params.forEach((name, value) -> normalised.append(name.trim()).append('=')
                    .append(value.trim()).append(';'));
        } catch (IllegalArgumentException e) {
            normalised.append(input == null ? "" : input.trim());   // not key=value: as given
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(normalised.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }

    /** The cached output for this work, counting a hit or a miss. */
    public Optional<String> lookup(TaskType type, String input) {
        String key = key(type, input);
        String output = memory.getIfPresent(key);
        if (output == null && database != null) {
            output = readThrough(key);
            if (output != null) {
                memory.put(key, output);
            }
        }
        if (output == null) {
            misses.incrementAndGet();
            return Optional.empty();
        }
        hits.incrementAndGet();
        history.cacheHit(key);
        return Optional.of(output);
    }

    /** Keeps a result the worker marked cacheable. */
    public void store(TaskType type, String input, String output) {
        String key = key(type, input);
        if (memory.asMap().putIfAbsent(key, output) == null) {
            stored.incrementAndGet();
            history.cacheStore(key, type.name(), output);
        }
    }

    private String readThrough(String key) {
        try (Connection connection = database.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT output FROM result_cache WHERE key = ?")) {
            statement.setString(1, key);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        } catch (SQLException e) {
            log.debug("Result cache table not read ({}): treating as a miss", e.getMessage());
            return null;
        }
    }

    public long hits() {
        return hits.get();
    }

    public long misses() {
        return misses.get();
    }

    public long stored() {
        return stored.get();
    }
}
