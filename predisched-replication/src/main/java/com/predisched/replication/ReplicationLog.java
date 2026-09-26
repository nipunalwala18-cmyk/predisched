package com.predisched.replication;

import java.util.ArrayList;
import java.util.List;

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
