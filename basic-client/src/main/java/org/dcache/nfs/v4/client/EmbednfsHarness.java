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
import java.util.Arrays;
import java.util.UUID;

import org.dcache.nfs.vfs.Stat;

/**
 * Non-interactive harness entrypoint intended for external interoperability
 * testing against embednfs.
 */
public final class EmbednfsHarness {

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

        Main client = new Main(new InetSocketAddress(host, port));
        try {
            client.mount(export);

            switch (mode) {
                case "smoke":
                    runSmoke(client);
                    break;
                case "stress":
                    throw new UnsupportedOperationException("stress mode not implemented yet");
                default:
                    throw new IllegalArgumentException("unknown mode: " + mode);
            }
        } finally {
            client.umount();
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

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return args[index];
    }

    private static void usage() {
        System.err.println("usage: EmbednfsHarness <smoke|stress> --host <host> --port <port> [--export /] [--pnfs off]");
    }
}
