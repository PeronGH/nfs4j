/*
 * Copyright (c) 2009 - 2025 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.nfs.v4.client;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dcache.nfs.status.NotSameException;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.Stat;

/**
 * Non-interactive harness entrypoint intended for external interoperability
 * testing against embednfs.
 */
public final class EmbednfsHarness {

    private static final int IO_WORKERS = 4;
    private static final int METADATA_WORKERS = 2;
    private static final int TRAVERSAL_WORKERS = 2;
    private static final int TOTAL_WORKERS = IO_WORKERS + METADATA_WORKERS + TRAVERSAL_WORKERS;

    private static final Duration STRESS_DURATION = Duration.ofSeconds(12);

    private static final int LARGE_FILE_SIZE = 32 * 1024 * 1024;
    private static final int LARGE_CHUNK_SIZE = 1_048_576;
    private static final int LARGE_CHUNK_COUNT = LARGE_FILE_SIZE / LARGE_CHUNK_SIZE;
    private static final int SAMPLE_SIZE = 4096;
    private static final int METADATA_FILE_COUNT = 6;
    private static final int METADATA_FILE_SIZE = 64 * 1024;

    private static final int MIN_IO_PASSES = 2;
    private static final int MIN_METADATA_CYCLES = 50;
    private static final int MIN_TRAVERSAL_SCANS = 100;
    private static final int STATUS_PAYLOAD_SIZE = fixedStatusPayload(0, 0).length;

    private EmbednfsHarness() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }

        String mode = args[0];
        String host = "127.0.0.1";
        int port = 2049;
        String export = "/";

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--host":
                    host = requireValue(args, ++i, "--host");
                    break;
                case "--port":
                    port = Integer.parseInt(requireValue(args, ++i, "--port"));
                    break;
                case "--export":
                    export = requireValue(args, ++i, "--export");
                    break;
                case "--pnfs":
                    String pnfs = requireValue(args, ++i, "--pnfs");
                    if (!"off".equalsIgnoreCase(pnfs)) {
                        throw new IllegalArgumentException("only --pnfs off is currently supported");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }

        switch (mode) {
            case "smoke":
                Main smokeClient = mountClient(host, port, export);
                try {
                    runSmoke(smokeClient);
                } finally {
                    safeUmount(smokeClient);
                }
                break;
            case "stress":
                runStress(host, port, export);
                break;
            default:
                throw new IllegalArgumentException("unknown mode: " + mode);
        }
    }

    private static Main mountClient(String host, int port, String export) throws Exception {
        Main client = new Main(new InetSocketAddress(host, port));
        try {
            client.mount(export);
            return client;
        } catch (Exception e) {
            try {
                safeUmount(client);
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    private static void runSmoke(Main client) throws Exception {
        String id = UUID.randomUUID().toString();
        String dir = "embednfs-harness-" + id + "-dir";
        String file = "embednfs-harness-" + id + ".txt";
        byte[] payload = "embednfs-harness-smoke".getBytes(StandardCharsets.UTF_8);

        try {
            client.mkdir(dir);
            client.writeFile(file, payload, 0);

            byte[] readBack = client.readFile(file, 0, payload.length);
            if (!Arrays.equals(readBack, payload)) {
                throw new IllegalStateException("payload mismatch");
            }

            Stat stat = client.statPath(file);
            if (stat.getSize() != payload.length) {
                throw new IllegalStateException("unexpected file size: " + stat.getSize());
            }

            String[] entries = client.listCurrent();
            if (Arrays.stream(entries).noneMatch(file::equals)) {
                throw new IllegalStateException("created file not visible in root listing");
            }
            if (Arrays.stream(entries).noneMatch(dir::equals)) {
                throw new IllegalStateException("created directory not visible in root listing");
            }

            System.out.println("smoke ok");
        } finally {
            try {
                client.remove(file);
            } catch (Exception ignored) {
            }
            try {
                client.remove(dir);
            } catch (Exception ignored) {
            }
        }
    }

    private static void runStress(String host, int port, String export) throws Exception {
        StressPaths paths = new StressPaths("/embednfs-harness-stress-" + UUID.randomUUID());
        Main setupClient = mountClient(host, port, export);
        try {
            setupStressTree(setupClient, paths);

            Instant deadline = Instant.now().plus(STRESS_DURATION);
            CyclicBarrier startBarrier = new CyclicBarrier(TOTAL_WORKERS);
            ExecutorService executor = Executors.newFixedThreadPool(TOTAL_WORKERS);
            List<Future<WorkerReport>> futures = new ArrayList<>(TOTAL_WORKERS);

            for (int workerId = 0; workerId < IO_WORKERS; workerId++) {
                final int id = workerId;
                futures.add(executor.submit(() -> ioWorker(id, host, port, export, paths, startBarrier, deadline)));
            }
            for (int workerId = 0; workerId < METADATA_WORKERS; workerId++) {
                final int id = workerId;
                futures.add(
                        executor.submit(() -> metadataWorker(id, host, port, export, paths, startBarrier, deadline)));
            }
            for (int workerId = 0; workerId < TRAVERSAL_WORKERS; workerId++) {
                final int id = workerId;
                futures.add(
                        executor.submit(() -> traversalWorker(id, host, port, export, paths, startBarrier, deadline)));
            }

            List<WorkerReport> reports = new ArrayList<>(TOTAL_WORKERS);
            try {
                for (Future<WorkerReport> future : futures) {
                    reports.add(future.get());
                }
            } finally {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }

            if (reports.size() != TOTAL_WORKERS) {
                throw new IllegalStateException("missing worker reports");
            }

            for (WorkerReport report : reports) {
                if (report.completed <= 0) {
                    throw new IllegalStateException(report.label + " worker " + report.id + " made no progress");
                }
                System.out.println(report.label + " worker " + report.id + " completed " + report.completed);
            }

            System.out.println("stress ok");
        } finally {
            try {
                cleanupStressTree(setupClient, paths);
            } finally {
                safeUmount(setupClient);
            }
        }
    }

    private static void setupStressTree(Main client, StressPaths paths) throws Exception {
        client.mkdirPath(paths.root());
        client.mkdirPath(paths.ioRoot());
        client.mkdirPath(paths.metaRoot());

        for (int workerId = 0; workerId < IO_WORKERS; workerId++) {
            client.mkdirPath(paths.ioDir(workerId));
            createHotFile(client, paths.hotFile(workerId), workerId, 0);
            writeNewFile(client, paths.statusFile(workerId), fixedStatusPayload(workerId, 0));
        }

        for (int workerId = 0; workerId < METADATA_WORKERS; workerId++) {
            String[] dirs = paths.metadataDirs(workerId);
            client.mkdirPath(dirs[0]);
            client.mkdirPath(dirs[1]);
            for (int fileIdx = 0; fileIdx < METADATA_FILE_COUNT; fileIdx++) {
                byte[] payload = filledBytes(metadataByte(workerId, 0, fileIdx), METADATA_FILE_SIZE);
                writeNewFile(client, dirs[0] + "/file-" + fileIdx + ".bin", payload);
            }
        }
    }

    private static void cleanupStressTree(Main client, StressPaths paths) throws Exception {
        for (int workerId = 0; workerId < IO_WORKERS; workerId++) {
            removeIfPresent(client, paths.hotFile(workerId));
            removeIfPresent(client, paths.statusFile(workerId));
            removeIfPresent(client, paths.ioDir(workerId));
        }

        for (int workerId = 0; workerId < METADATA_WORKERS; workerId++) {
            String[] dirs = paths.metadataDirs(workerId);
            removeDirectoryContents(client, dirs[0]);
            removeDirectoryContents(client, dirs[1]);
            removeIfPresent(client, dirs[0]);
            removeIfPresent(client, dirs[1]);
        }

        removeIfPresent(client, paths.ioRoot());
        removeIfPresent(client, paths.metaRoot());
        removeIfPresent(client, paths.root());
    }

    private static void removeDirectoryContents(Main client, String dir) throws Exception {
        try {
            for (String name : client.listPath(dir)) {
                removeIfPresent(client, dir + "/" + name);
            }
        } catch (Exception e) {
            if (!isMissingPath(e)) {
                throw e;
            }
        }
    }

    private static void removeIfPresent(Main client, String path) throws Exception {
        try {
            client.removePath(path);
        } catch (Exception e) {
            if (!isMissingPath(e)) {
                throw e;
            }
        }
    }

    private static WorkerReport ioWorker(
            int workerId,
            String host,
            int port,
            String export,
            StressPaths paths,
            CyclicBarrier startBarrier,
            Instant deadline) throws Exception {
        Main client = null;
        try {
            client = mountClient(host, port, export);
            String hotPath = paths.hotFile(workerId);
            String statusPath = paths.statusFile(workerId);
            int passes = 0;

            await(startBarrier);
            while (Instant.now().isBefore(deadline)) {
                int pass = passes + 1;
                writeHotFilePass(client, hotPath, workerId, pass);

                Stat attrs = client.statPath(hotPath);
                if (attrs.getSize() != LARGE_FILE_SIZE) {
                    throw new IllegalStateException("unexpected size for " + hotPath + ": " + attrs.getSize());
                }

                assertHotFileSamples(client, hotPath, workerId, pass);
                if (pass % 4 == 0) {
                    assertFullHotFile(client, hotPath, workerId, pass);
                }

                writeExistingFile(client, statusPath, fixedStatusPayload(workerId, pass), 0);
                passes++;
            }

            if (passes < MIN_IO_PASSES) {
                throw new IllegalStateException("io worker " + workerId + " completed only " + passes + " passes");
            }

            return new WorkerReport("io", workerId, passes);
        } catch (Exception e) {
            System.err.println("io worker " + workerId + " failed: " + e);
            abortBarrier(startBarrier);
            throw e;
        } finally {
            if (client != null) {
                safeUmount(client);
            }
        }
    }

    private static WorkerReport metadataWorker(
            int workerId,
            String host,
            int port,
            String export,
            StressPaths paths,
            CyclicBarrier startBarrier,
            Instant deadline) throws Exception {
        Main client = null;
        try {
            client = mountClient(host, port, export);
            String[] dirs = paths.metadataDirs(workerId);
            String dirA = dirs[0];
            String dirB = dirs[1];
            boolean[] inDirA = new boolean[METADATA_FILE_COUNT];
            Arrays.fill(inDirA, true);
            int cycles = 0;

            await(startBarrier);
            while (Instant.now().isBefore(deadline)) {
                int primary = cycles % METADATA_FILE_COUNT;
                int secondary = (cycles + 3) % METADATA_FILE_COUNT;

                String primaryFromDir = inDirA[primary] ? dirA : dirB;
                String primaryToDir = inDirA[primary] ? dirB : dirA;
                String primaryFromPath = primaryFromDir + "/file-" + primary + ".bin";
                String primaryToPath = primaryToDir + "/file-" + primary + ".bin";

                client.renamePath(primaryFromPath, primaryToPath);
                inDirA[primary] = !inDirA[primary];

                writeExistingFile(
                        client,
                        primaryToPath,
                        filledBytes(metadataByte(workerId, cycles, primary), METADATA_FILE_SIZE),
                        0);
                Stat attrs = client.statPath(primaryToPath);
                if (attrs.getSize() != METADATA_FILE_SIZE) {
                    throw new IllegalStateException("unexpected size for " + primaryToPath + ": " + attrs.getSize());
                }

                String secondaryCurrentDir = inDirA[secondary] ? dirA : dirB;
                String secondaryNextDir = inDirA[secondary] ? dirB : dirA;
                String secondaryCurrentPath = secondaryCurrentDir + "/file-" + secondary + ".bin";
                String secondaryNextPath = secondaryNextDir + "/file-" + secondary + ".bin";

                client.removePath(secondaryCurrentPath);
                writeNewFile(
                        client,
                        secondaryNextPath,
                        filledBytes(metadataByte(workerId, cycles + 1, secondary), METADATA_FILE_SIZE));

                byte[] sample = readExactFile(client, secondaryNextPath, 0, SAMPLE_SIZE);
                byte expected = metadataByte(workerId, cycles + 1, secondary);
                if (!allBytesEqual(sample, expected)) {
                    throw new IllegalStateException(
                            "sample mismatch in " + secondaryNextPath + " for cycle " + cycles);
                }
                inDirA[secondary] = !inDirA[secondary];

                List<String> namesA = listSorted(client, dirA);
                List<String> namesB = listSorted(client, dirB);
                if (namesA.size() > METADATA_FILE_COUNT || namesB.size() > METADATA_FILE_COUNT) {
                    throw new IllegalStateException(
                            "metadata directory entry count exceeded bounds: "
                                    + namesA.size() + " + " + namesB.size());
                }
                if (namesA.size() + namesB.size() != METADATA_FILE_COUNT) {
                    throw new IllegalStateException(
                            "metadata directory entry count drifted: "
                                    + namesA.size() + " + " + namesB.size() + " != " + METADATA_FILE_COUNT);
                }

                cycles++;
            }

            if (cycles < MIN_METADATA_CYCLES) {
                throw new IllegalStateException(
                        "metadata worker " + workerId + " completed only " + cycles + " cycles");
            }

            return new WorkerReport("metadata", workerId, cycles);
        } catch (Exception e) {
            System.err.println("metadata worker " + workerId + " failed: " + e);
            abortBarrier(startBarrier);
            throw e;
        } finally {
            if (client != null) {
                safeUmount(client);
            }
        }
    }

    private static WorkerReport traversalWorker(
            int workerId,
            String host,
            int port,
            String export,
            StressPaths paths,
            CyclicBarrier startBarrier,
            Instant deadline) throws Exception {
        Main client = null;
        try {
            client = mountClient(host, port, export);
            List<String> expectedRoot = Arrays.asList("io", "meta");
            List<String> expectedIoRoot = new ArrayList<>(IO_WORKERS);
            for (int ioWorkerId = 0; ioWorkerId < IO_WORKERS; ioWorkerId++) {
                expectedIoRoot.add("io-" + ioWorkerId);
            }
            List<String> expectedMetaRoot = new ArrayList<>(METADATA_WORKERS * 2);
            for (int metaWorkerId = 0; metaWorkerId < METADATA_WORKERS; metaWorkerId++) {
                expectedMetaRoot.add("meta-" + metaWorkerId + "-a");
                expectedMetaRoot.add("meta-" + metaWorkerId + "-b");
            }
            expectedIoRoot.sort(String::compareTo);
            expectedMetaRoot.sort(String::compareTo);

            int scans = 0;
            await(startBarrier);
            while (Instant.now().isBefore(deadline)) {
                if (!listSorted(client, paths.root()).equals(expectedRoot)) {
                    throw new IllegalStateException("root directory listing changed unexpectedly");
                }
                if (!listSorted(client, paths.ioRoot()).equals(expectedIoRoot)) {
                    throw new IllegalStateException("io root directory listing changed unexpectedly");
                }
                if (!listSorted(client, paths.metaRoot()).equals(expectedMetaRoot)) {
                    throw new IllegalStateException("meta root directory listing changed unexpectedly");
                }

                for (int ioWorkerId = 0; ioWorkerId < IO_WORKERS; ioWorkerId++) {
                    List<String> entries = listSorted(client, paths.ioDir(ioWorkerId));
                    if (!entries.equals(Arrays.asList("hot.bin", "status.txt"))) {
                        throw new IllegalStateException("unexpected entries in " + paths.ioDir(ioWorkerId));
                    }

                    Stat hotStat = client.statPath(paths.hotFile(ioWorkerId));
                    if (hotStat.getSize() != LARGE_FILE_SIZE) {
                        throw new IllegalStateException(
                                "unexpected hot file size in " + paths.hotFile(ioWorkerId) + ": " + hotStat.getSize());
                    }

                    long sampleOffset =
                            (long) ((scans + workerId + ioWorkerId) % LARGE_CHUNK_COUNT) * LARGE_CHUNK_SIZE;
                    byte[] sample = readExactFile(client, paths.hotFile(ioWorkerId), sampleOffset, SAMPLE_SIZE);
                    if (sample.length != SAMPLE_SIZE) {
                        throw new IllegalStateException("short hot file sample in " + paths.hotFile(ioWorkerId));
                    }

                    byte[] status = readExactFile(client, paths.statusFile(ioWorkerId), 0, STATUS_PAYLOAD_SIZE);
                    if (!new String(status, StandardCharsets.UTF_8).startsWith("worker=")) {
                        throw new IllegalStateException("unexpected status payload in " + paths.statusFile(ioWorkerId));
                    }
                }

                for (int metadataWorkerId = 0; metadataWorkerId < METADATA_WORKERS; metadataWorkerId++) {
                    String[] dirs = paths.metadataDirs(metadataWorkerId);
                    List<String> namesA;
                    List<String> namesB;
                    try {
                        namesA = listSorted(client, dirs[0]);
                        namesB = listSorted(client, dirs[1]);
                    } catch (NotSameException e) {
                        continue;
                    }

                    if (namesA.size() > METADATA_FILE_COUNT || namesB.size() > METADATA_FILE_COUNT) {
                        throw new IllegalStateException(
                                "metadata scan found too many entries: " + namesA.size() + " + " + namesB.size());
                    }

                    List<String> probeNames = new ArrayList<>(namesA);
                    probeNames.addAll(namesB);
                    int probeCount = Math.min(2, probeNames.size());
                    for (int i = 0; i < probeCount; i++) {
                        String name = probeNames.get(i);
                        String path = probeMetadataFile(client, dirs[0], dirs[1], name);
                        if (path == null) {
                            continue;
                        }

                        Stat attrs;
                        try {
                            attrs = client.statPath(path);
                        } catch (Exception e) {
                            if (isTransientMetadataError(e)) {
                                continue;
                            }
                            throw e;
                        }
                        if (attrs.getSize() != METADATA_FILE_SIZE) {
                            continue;
                        }

                        byte[] sample;
                        try {
                            sample = readExactFile(client, path, 0, SAMPLE_SIZE);
                        } catch (Exception e) {
                            if (isTransientMetadataError(e)) {
                                continue;
                            }
                            throw e;
                        }
                        if (sample.length != SAMPLE_SIZE) {
                            throw new IllegalStateException("short metadata sample in " + path);
                        }
                    }
                }

                scans++;
            }

            if (scans < MIN_TRAVERSAL_SCANS) {
                throw new IllegalStateException(
                        "traversal worker " + workerId + " completed only " + scans + " scans");
            }

            return new WorkerReport("traversal", workerId, scans);
        } catch (Exception e) {
            System.err.println("traversal worker " + workerId + " failed: " + e);
            abortBarrier(startBarrier);
            throw e;
        } finally {
            if (client != null) {
                safeUmount(client);
            }
        }
    }

    private static void createHotFile(Main client, String path, int workerId, int pass) throws Exception {
        Main.OpenReply open = client.create(path);
        try {
            writeHotFilePass(client, open, workerId, pass);
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static void writeHotFilePass(Main client, String path, int workerId, int pass) throws Exception {
        Main.OpenReply open = client.open(path, nfs4_prot.OPEN4_SHARE_ACCESS_BOTH);
        try {
            writeHotFilePass(client, open, workerId, pass);
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static void writeHotFilePass(Main client, Main.OpenReply open, int workerId, int pass) throws Exception {
        byte[] chunk = new byte[LARGE_CHUNK_SIZE];
        for (int chunkIdx = 0; chunkIdx < LARGE_CHUNK_COUNT; chunkIdx++) {
            Arrays.fill(chunk, patternByte(workerId, pass, chunkIdx));
            writeAll(client, open, chunk, (long) chunkIdx * LARGE_CHUNK_SIZE);
        }
    }

    private static void writeNewFile(Main client, String path, byte[] data) throws Exception {
        Main.OpenReply open = client.create(path);
        try {
            writeAll(client, open, data, 0);
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static void writeExistingFile(Main client, String path, byte[] data, long offset) throws Exception {
        Main.OpenReply open = client.open(path, nfs4_prot.OPEN4_SHARE_ACCESS_BOTH);
        try {
            writeAll(client, open, data, offset);
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static void writeAll(Main client, Main.OpenReply open, byte[] data, long offset) throws Exception {
        int written = 0;
        while (written < data.length) {
            byte[] remaining = written == 0 ? data : Arrays.copyOfRange(data, written, data.length);
            int count = client.writeBytes(open.fh(), remaining, offset + written, open.stateid());
            if (count <= 0) {
                throw new IllegalStateException("short write at offset " + (offset + written));
            }
            written += count;
        }
    }

    private static byte[] readExactFile(Main client, String path, long offset, int count) throws Exception {
        Main.OpenReply open = client.open(path);
        try {
            return readExact(client, open, offset, count);
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static byte[] readExact(Main client, Main.OpenReply open, long offset, int count) throws Exception {
        byte[] data = new byte[count];
        int filled = 0;
        long cursor = offset;
        while (filled < count) {
            int request = Math.min(count - filled, LARGE_CHUNK_SIZE);
            byte[] chunk = client.readBytes(open.fh(), open.stateid(), request, cursor);
            if (chunk.length == 0) {
                throw new IllegalStateException("read returned EOF after " + filled + " of " + count + " bytes");
            }
            System.arraycopy(chunk, 0, data, filled, chunk.length);
            filled += chunk.length;
            cursor += chunk.length;
        }
        return data;
    }

    private static void assertHotFileSamples(Main client, String path, int workerId, int pass) throws Exception {
        Main.OpenReply open = client.open(path);
        try {
            for (int chunkIdx : new int[] {0, 8, 16, 24}) {
                long offset = (long) chunkIdx * LARGE_CHUNK_SIZE;
                byte[] sample = readExact(client, open, offset, SAMPLE_SIZE);
                byte expected = patternByte(workerId, pass, chunkIdx);
                if (!allBytesEqual(sample, expected)) {
                    throw new IllegalStateException(
                            "sample mismatch in " + path + " at chunk " + chunkIdx + " for pass " + pass);
                }
            }
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static void assertFullHotFile(Main client, String path, int workerId, int pass) throws Exception {
        Main.OpenReply open = client.open(path);
        try {
            for (int chunkIdx = 0; chunkIdx < LARGE_CHUNK_COUNT; chunkIdx++) {
                long offset = (long) chunkIdx * LARGE_CHUNK_SIZE;
                byte[] chunk = readExact(client, open, offset, LARGE_CHUNK_SIZE);
                byte expected = patternByte(workerId, pass, chunkIdx);
                if (!allBytesEqual(chunk, expected)) {
                    throw new IllegalStateException(
                            "full-read mismatch in " + path + " at chunk " + chunkIdx + " for pass " + pass);
                }
            }
        } finally {
            client.close(open.fh(), open.stateid());
        }
    }

    private static List<String> listSorted(Main client, String path) throws Exception {
        List<String> entries = new ArrayList<>(Arrays.asList(client.listPath(path)));
        entries.sort(String::compareTo);
        return entries;
    }

    private static String probeMetadataFile(Main client, String dirA, String dirB, String name) throws Exception {
        String pathA = dirA + "/" + name;
        try {
            client.statPath(pathA);
            return pathA;
        } catch (Exception e) {
            if (!isTransientMetadataError(e)) {
                throw e;
            }
        }

        String pathB = dirB + "/" + name;
        try {
            client.statPath(pathB);
            return pathB;
        } catch (Exception e) {
            if (!isTransientMetadataError(e)) {
                throw e;
            }
        }

        return null;
    }

    private static void await(CyclicBarrier startBarrier) throws InterruptedException, BrokenBarrierException {
        startBarrier.await();
    }

    private static void abortBarrier(CyclicBarrier startBarrier) {
        startBarrier.reset();
    }

    private static boolean allBytesEqual(byte[] data, byte expected) {
        for (byte value : data) {
            if (value != expected) {
                return false;
            }
        }
        return true;
    }

    private static byte[] filledBytes(byte value, int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, value);
        return data;
    }

    private static byte patternByte(int workerId, int pass, int chunkIdx) {
        return (byte) (((workerId * 31L + pass * 17L + chunkIdx * 7L) % 251) + 1);
    }

    private static byte metadataByte(int workerId, int cycle, int fileIdx) {
        return (byte) (((workerId * 19L + cycle * 11L + fileIdx * 5L) % 251) + 1);
    }

    private static byte[] fixedStatusPayload(int workerId, int pass) {
        return String.format("worker=%02d pass=%08d%n", workerId, pass).getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isTransientMetadataError(Throwable error) {
        String message = collectMessages(error);
        return message.contains("no such file")
                || message.contains("nfs4err_noent")
                || message.contains("invalid file handle")
                || message.contains("nfs4err_badhandle")
                || message.contains("stale");
    }

    private static boolean isMissingPath(Throwable error) {
        String message = collectMessages(error);
        return message.contains("no such file")
                || message.contains("nfs4err_noent")
                || message.contains("not found")
                || message.contains("invalid file handle")
                || message.contains("nfs4err_badhandle")
                || message.contains("stale");
    }

    private static void safeUmount(Main client) throws Exception {
        try {
            client.umount();
        } catch (Exception e) {
            if (!collectMessages(e).contains("nfs4err_clientid_busy")) {
                throw e;
            }
        }
    }

    private static String collectMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null) {
                if (builder.length() > 0) {
                    builder.append(' ');
                }
                builder.append(current.getMessage().toLowerCase());
            }
            current = current.getCause();
        }
        return builder.toString();
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return args[index];
    }

    private static void usage() {
        System.err.println("usage: EmbednfsHarness <smoke|stress> --host <host> --port <port> [--export /] [--pnfs off]");
    }

    private static final class WorkerReport {

        private final String label;
        private final int id;
        private final int completed;

        private WorkerReport(String label, int id, int completed) {
            this.label = label;
            this.id = id;
            this.completed = completed;
        }
    }

    private static final class StressPaths {

        private final String root;
        private final String ioRoot;
        private final String metaRoot;

        private StressPaths(String root) {
            this.root = root;
            this.ioRoot = root + "/io";
            this.metaRoot = root + "/meta";
        }

        private String root() {
            return root;
        }

        private String ioRoot() {
            return ioRoot;
        }

        private String metaRoot() {
            return metaRoot;
        }

        private String ioDir(int workerId) {
            return ioRoot + "/io-" + workerId;
        }

        private String hotFile(int workerId) {
            return ioDir(workerId) + "/hot.bin";
        }

        private String statusFile(int workerId) {
            return ioDir(workerId) + "/status.txt";
        }

        private String[] metadataDirs(int workerId) {
            return new String[] {
                    metaRoot + "/meta-" + workerId + "-a",
                    metaRoot + "/meta-" + workerId + "-b"
            };
        }
    }
}
