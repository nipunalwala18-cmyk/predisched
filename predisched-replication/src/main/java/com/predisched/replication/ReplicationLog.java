package com.predisched.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Every change this replica has applied, in order, numbered from 1. A replica that was down or
 * is new asks for everything after the last sequence number it has ({@code SyncFrom}).
 * Appends and reads are serialised on this object; entries are immutable.
 */
public class ReplicationLog {

    /** A logged change and its sequence number in this log. */
    public record Entry(long seqNo, VersionedRecord record) {}

    private final List<Entry> entries = new ArrayList<>();

    /** Appends a change and returns its sequence number. */
    public synchronized long append(VersionedRecord record) {
        long seqNo = entries.size() + 1L;
        entries.add(new Entry(seqNo, record));
        return seqNo;
    }

    /**
     * Appends a change at exactly {@code seqNo}, which must directly follow the last entry.
     * Primary-backup (prompt 10) numbers changes on the primary, and every backup's log holds
     * them at the same positions.
     */
    public synchronized void appendAt(long seqNo, VersionedRecord record) {
        if (seqNo != entries.size() + 1L) {
            throw new IllegalStateException(
                    "seq " + seqNo + " does not follow the last entry " + entries.size());
        }
        entries.add(new Entry(seqNo, record));
    }

    /** The entry at {@code seqNo}, if the log reaches that far. */
    public synchronized Optional<Entry> entry(long seqNo) {
        if (seqNo < 1 || seqNo > entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) (seqNo - 1)));
    }

    /** Drops every entry, before a full resync. */
    public synchronized void clear() {
        entries.clear();
    }

    /** Entries with a sequence number of at least {@code fromSeq}, oldest first. */
    public synchronized List<Entry> from(long fromSeq) {
        int start = (int) Math.max(0, Math.min(entries.size(), fromSeq - 1));
        return new ArrayList<>(entries.subList(start, entries.size()));
    }

    /** The last sequence number, 0 when empty. */
    public synchronized long lastSeq() {
        return entries.size();
    }
}
