package com.predisched.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.sql.DataSource;

/**
 * SLA compliance (F3, FR24) from the {@code tasks} table: of the tasks that carried a deadline,
 * how many completed by it, per scheduling strategy and per task type.
 *
 * <p>Definition: a task with a deadline is <em>on time</em> if it COMPLETED no later than its
 * deadline, <em>late</em> if it finished any other way (completed after it, failed, cancelled),
 * and <em>open</em> while unfinished. On-time % is on time / (on time + late); open tasks are
 * not counted either way.
 */
public final class SlaReport {

    /** One group's counts. */
    public record Row(String key, long onTime, long late, long open) {

        public long finished() {
            return onTime + late;
        }

        /** On-time percentage of finished tasks; NaN when none finished. */
        public double onTimePct() {
            return finished() == 0 ? Double.NaN : 100.0 * onTime / finished();
        }
    }

    private SlaReport() {}

    public static List<Row> byStrategy(DataSource db, Instant since) throws SQLException {
        return query(db, "coalesce(strategy, '(not dispatched)')", since);
    }

    public static List<Row> byTaskType(DataSource db, Instant since) throws SQLException {
        return query(db, "type", since);
    }

    private static List<Row> query(DataSource db, String groupBy, Instant since)
            throws SQLException {
        String sql = "SELECT " + groupBy + " AS k,"
                + " count(*) FILTER (WHERE sla_met) AS on_time,"
                + " count(*) FILTER (WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')"
                + "     AND NOT coalesce(sla_met, false)) AS late,"
                + " count(*) FILTER (WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED'))"
                + "     AS open"
                + " FROM tasks WHERE deadline_at IS NOT NULL AND submitted_at >= ?"
                + " GROUP BY k ORDER BY k";
        List<Row> rows = new ArrayList<>();
        try (Connection connection = db.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(since));
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    rows.add(new Row(result.getString("k"), result.getLong("on_time"),
                            result.getLong("late"), result.getLong("open")));
                }
            }
        }
        return rows;
    }

    /** Both tables, as the CLI prints them. */
    public static String format(List<Row> byStrategy, List<Row> byTaskType) {
        StringBuilder out = new StringBuilder();
        table(out, "strategy", byStrategy);
        out.append('\n');
        table(out, "task type", byTaskType);
        return out.toString();
    }

    private static void table(StringBuilder out, String title, List<Row> rows) {
        out.append(String.format(Locale.ROOT, "%-18s %8s %8s %8s %6s  %s%n",
                title, "on time", "late", "open", "", "on-time %"));
        if (rows.isEmpty()) {
            out.append("  (no tasks with a deadline)\n");
            return;
        }
        long onTime = 0;
        long late = 0;
        long open = 0;
        for (Row row : rows) {
            out.append(line(row.key(), row));
            onTime += row.onTime();
            late += row.late();
            open += row.open();
        }
        out.append(line("all", new Row("all", onTime, late, open)));
    }

    private static String line(String key, Row row) {
        double pct = row.onTimePct();
        return String.format(Locale.ROOT, "%-18s %8d %8d %8d %6s  %s%n", key, row.onTime(),
                row.late(), row.open(), "",
                Double.isNaN(pct) ? "-" : String.format(Locale.ROOT, "%.1f", pct));
    }
}
