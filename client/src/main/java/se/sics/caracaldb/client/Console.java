/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.caracaldb.client;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import jline.console.ConsoleReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.global.Schema;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.RangeResponse;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.utils.ByteArrayFormatter;
import util.log4j.ColoredPatternLayout;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Console {

    private ConsoleReader reader;
    private PrintWriter out;
    private Config conf;
    private final Map<String, Command> commands = new HashMap<String, Command>();
    private final int padTo;

    {
        commands.put("get", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    String[] kvline = cmdline[1].split(" ", 2);
                    if (kvline.length != 2) {
                        return false;
                    }
                    String schema = kvline[0];
                    Key k = Key.fromHex(correctFormat(kvline[1]));
                    out.println("Getting " + k.toString() + "...");
                    GetResponse resp = worker.get(schema, k);
                    if (resp.code == ResponseCode.SUCCESS) {
                        out.println("success!");
                        if (resp.data != null) {
                            out.println("   " + k.toString() + "->" + new String(resp.data));
                        } else {
                            out.println("   " + k.toString() + "->(null)");
                        }
                        return true;
                    } else {
                        out.println("Result: " + resp.code.name());
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String usage() {
                return "get <schema> <key>";
            }

            @Override
            public String help() {
                return "gets the current <value> for <key>";
            }
        });
        commands.put("put", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    String[] kvline = cmdline[1].split(" ", 3);
                    if (kvline.length < 2 || kvline.length > 3) {
                        return false;
                    }
                    String schema = kvline[0];
                    Key k = Key.fromHex(correctFormat(kvline[1]));
                    byte[] value = null;
                    if (kvline.length == 3) {
                        value = kvline[2].getBytes();
                    }
                    out.println("Setting " + k.toString() + " to 0x" + ByteArrayFormatter.toHexString(value) + "...");
                    ResponseCode resp = worker.put(schema, k, value);
                    out.println("Result: " + resp.name());
                    return true;
                }
                return false;
            }

            @Override
            public String usage() {
                return "put <schema> <key> <value>";
            }

            @Override
            public String help() {
                return "sets <key> to <value>";
            }
        });
        commands.put("system", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                out.println(worker.getSystemInfo());
                return true;
            }

            @Override
            public String usage() {
                return "system";
            }

            @Override
            public String help() {
                return "show full system stats (with LUT only)";
            }
        });
        commands.put("schemas", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                out.println("Schemas:");
                for (String schema : worker.listSchemas()) {
                    out.println("   " + schema);
                }
                return true;
            }

            @Override
            public String usage() {
                return "schemas";
            }

            @Override
            public String help() {
                return "lists all schemas currently known";
            }
        });
        commands.put("info", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    out.println("Schema Info:");
                    out.println(worker.getSchemaInfo(cmdline[1]));
                    return true;
                }
                return false;
            }

            @Override
            public String usage() {
                return "info <schema>";
            }

            @Override
            public String help() {
                return "lists schema meta data if exists";
            }
        });
        commands.put("create", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    String[] kvline = cmdline[1].split(" ");
                    String schema = kvline[0];
                    ImmutableMap.Builder<String, String> meta = ImmutableMap.builder();
                    for (int i = 1; i < kvline.length; i++) {
                        String[] paramline = kvline[i].split(":", 2);
                        if (paramline.length != 2) {
                            out.print("Invalid parameter format: " + kvline[i]);
                            return false;
                        }
                        meta.put(paramline[0], paramline[1]);
                    }
                    Future<Schema.Response> f = worker.createSchema(schema, meta.build());
                    out.println("Creating schema " + schema + "()...this might take a while...");
                    try {
                        Schema.Response res = f.get();
                        if (res.success) {
                            out.println("Created schema " + res.name + " with id " + ByteArrayFormatter.toHexString(res.id));
                        } else {
                            out.println("Could not create schema " + res.name + ". Message was: \n" + res.msg);
                        }
                    } catch (InterruptedException ex) {
                        out.println("Couldn't get response. Error was: " + ex.getMessage());
                    } catch (ExecutionException ex) {
                        out.println("Couldn't get response. Error was: " + ex.getMessage());
                    }
                    return true;
                }
                return false;
            }

            @Override
            public String usage() {
                return "create <schema> <param>:<value>...";
            }

            @Override
            public String help() {
                return "creates a new schema with the param:value pairs as meta data";
            }
        });
        commands.put("drop", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    Future<Schema.Response> f = worker.dropSchema(cmdline[1]);
                    out.println("Dropping schema " + cmdline[1] + "()...this might take a while...");
                    try {
                        Schema.Response res = f.get();
                        if (res.success) {
                            out.println("Dropped schema " + res.name);
                        } else {
                            out.println("Could not drop schema " + res.name + ". Message was: \n" + res.msg);
                        }
                    } catch (InterruptedException ex) {
                        out.println("Couldn't get response. Error was: " + ex.getMessage());
                    } catch (ExecutionException ex) {
                        out.println("Couldn't get response. Error was: " + ex.getMessage());
                    }
                    return true;
                }
                return false;
            }

            @Override
            public String usage() {
                return "drop <schema>";
            }

            @Override
            public String help() {
                return "deletes schema if it exists";
            }
        });
        commands.put("selectAll", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                if (cmdline.length == 2) {
                    KeyRange r = KeyRange.closed(Key.NULL_KEY).open(Key.INF);
                    RangeResponse res = worker.rangeRequest(cmdline[1], r);
                    if (res.code == ResponseCode.SUCCESS) {
                        out.println("sucess!");
                        for (Entry<Key, byte[]> e : res.results.entrySet()) {
                            Key k = e.getKey();
                            byte[] val = e.getValue();
                            if (val != null) {
                                out.println("   " + k.toString() + "->" + new String(val));
                            } else {
                                out.println("   " + k.toString() + "->(null)");
                            }
                        }
                    } else {
                        out.println("Could not execute request. Error was: " + res.code.name());
                    }
                    return true;
                }
                return false;
            }

            @Override
            public String usage() {
                return "selectAll <schema>";
            }

            @Override
            public String help() {
                return "executed a range query on the whole schema (mostly for debugging)";
            }
        });
        commands.put("help", new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                //out.println("Interface currently connected to " + bootstrapAddr + ":" + bootstrapPort + ".\n\n");
                out.println("Available commands: \n\n");
                Set<Command> comSet = new HashSet<Command>(commands.values());
                StringBuilder sb = new StringBuilder();
                for (Command c : comSet) {
                    sb.append(c.usage());
                    for (int i = c.usage().length(); i < padTo; i++) {
                        sb.append(' ');
                    }
                    sb.append(c.help());
                    sb.append('\n');
                }
                out.println(sb.toString());
                return true;
            }

            @Override
            public String usage() {
                return "help";
            }

            @Override
            public String help() {
                return "shows this help";
            }
        });
        Command exitcom = new Command() {

            @Override
            public boolean execute(String[] cmdline, BlockingClient worker) {
                out.println("Exiting...");
                System.exit(0);
                return true; // clearly an unreachable statement, mr java compiler -.-
            }

            @Override
            public String usage() {
                return "exit|quit";
            }

            @Override
            public String help() {
                return "closes the shell";
            }
        };
        commands.put("exit", exitcom);
        commands.put("quit", exitcom);
        int longestCom = 0;
        Set<Command> comSet = new HashSet<Command>(commands.values());
        for (Command c : comSet) {
            int useLength = c.usage().length();
            if (useLength > longestCom) {
                longestCom = useLength;
            }
        }
        padTo = longestCom + 4;
    }

    public static void main(String[] args) throws IOException {
        Console c = new Console();
        c.start();
    }

    public Console() throws IOException {
        reader = new ConsoleReader();
        out = new PrintWriter(reader.getOutput());
        PatternLayout layout = new ColoredPatternLayout("%d{[HH:mm:ss,SSS]} %-5p {%c{1}} %m%n");
        Logger rootL = LogManager.getRootLogger();
        rootL.removeAllAppenders();
        rootL.addAppender(new WriterAppender(layout, out));
        conf = ConfigFactory.load();
    }

    public void start() throws IOException {
        reader.setPrompt("CaracalDB@disconnected> ");

        String line;

        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            String[] sline = line.split(" ", 2);
            String cmd = sline[0];
            out.println("Command: " + cmd);
            if (cmd.equalsIgnoreCase("connect")) {
                if (sline.length == 2) {
                    connect(sline[1]);
                } else {
                    out.println("'connect' takes \"bootstrapaddress:port clientaddress:port\" parameters!");
                }
            } else if (cmd.equalsIgnoreCase("config")) {
                if (sline.length == 1) {
                    printConfig();
                } else {
                    handleConfigCmd(sline[1]);
                }
            } else if (cmd.equalsIgnoreCase("help")) {
                out.println("Interface currently disconnected use 'connect' to establish server connection.\n\n");
                out.println("Available commands: \n\n");
                out.println("connect {bootstrapaddress}:{port} {clientaddress}:{port}       connects to a server");
                out.println("config show|list|get <key>|set <key> <value>                   edits or queries the current configuration");
                out.println("help                                                           shows this help");
                out.println("exit|quit                                                      closes to shell");
            } else if (cmd.equalsIgnoreCase("exit") || cmd.equalsIgnoreCase("quit")) {
                out.println("Exiting...");
                System.exit(0);
            }
        }
        System.out.println("No more input...exiting.");
        System.exit(0);
    }

    private void connect(String params) throws IOException {
        String[] sline = params.split(" ", 2);
        if (sline.length != 2) {
            out.println("'connect' takes \"bootstrapaddress:port clientaddress:port\" parameters!");
            return;
        }
        String[] bootstrapLine = sline[0].split(":", 2);
        String[] clientLine = sline[1].split(":", 2);
        String bootstrapAddr = bootstrapLine[0];
        int bootstrapPort = Integer.parseInt(bootstrapLine[1]);
        String clientAddr = clientLine[0];
        int clientPort = Integer.parseInt(clientLine[1]);

        out.println("Connecting from " + clientAddr + ":" + clientPort + " to Bootstrap Server at " + bootstrapAddr + ":" + bootstrapPort + "...");
        Config serverConf = conf.withValue("bootstrap.address.hostname", ConfigValueFactory.fromAnyRef(bootstrapAddr));
        serverConf = serverConf.withValue("bootstrap.address.port", ConfigValueFactory.fromAnyRef(bootstrapPort));
        serverConf = serverConf.withValue("client.address.hostname", ConfigValueFactory.fromAnyRef(clientAddr));
        serverConf = serverConf.withValue("client.address.port", ConfigValueFactory.fromAnyRef(clientPort));

        //printConfig(serverConf);
        ClientManager.setConfig(serverConf);
        BlockingClient worker = ClientManager.newClient();

        int trials = 0;
        try {
            while (trials < 10) {
                Thread.sleep(1000);
                if (worker.test()) {
                    break;
                }
                out.println(trials + "...");
                trials++;
            }
            if (trials >= 10) {
                out.println("fail!");
                return;
            }
        } catch (InterruptedException ex) {
            out.println("interrupted!");
            return;
        }
        out.println("success!");

        reader.setPrompt("CaracalDB@" + bootstrapAddr + ":" + bootstrapPort + "> ");

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            String[] cmdline = line.split(" ", 2);
            String cmd = cmdline[0];
            cmd = cmd.toLowerCase();
            Command c = commands.get(cmd);
            if (c != null) {
                if (!c.execute(cmdline, worker)) {
                    out.print("Usage: ");
                    out.println(c.usage());
                }
            } else {
                out.println("Unkown command: " + cmd + " (use 'help' to see available commands)");
            }
        }
    }

    private void handleConfigCmd(String line) {
        String[] sline = line.split(" ", 2);
        String cmd = sline[0];
        if (cmd.equalsIgnoreCase("show") || cmd.equalsIgnoreCase("list")) {
            printConfig();
        } else if (cmd.equalsIgnoreCase("get")) {
            if (sline.length == 2) {
                out.println("   " + sline[1] + "->" + conf.getValue(sline[1]).render());
            } else {
                out.println("Usage: config get <key>");
            }
        } else if (cmd.equalsIgnoreCase("set")) {
            if (sline.length == 2) {
                String[] kvline = sline[1].split(" ", 2);
                String key = kvline[0];
                if (kvline.length == 2) {
                    String value = kvline[1];
                    ConfigValue val = null;
                    if (value.startsWith("\"")) {
                        val = ConfigValueFactory.fromAnyRef(value.substring(1, value.length() - 1));
                    } else {
                        val = ConfigValueFactory.fromAnyRef(Long.parseLong(value));
                    }
                    conf = conf.withValue(key, val);
                    out.println("Updated config. New value:");
                    out.println("   " + key + "->" + conf.getValue(key).render());
                } else {
                    out.println("Usage: config set <key> <value>");
                }
            } else {
                out.println("Usage: config set <key> <value>");
            }
        } else {
            out.println("Usage: config show|list|get <key>|set <key> <value>");
        }
    }

    private void printConfig() {
        printConfig(conf);
    }

    private void printConfig(Config someConf) {
        for (Entry<String, ConfigValue> e : someConf.entrySet()) {
            out.println("   " + e.getKey() + "->" + e.getValue().render());
        }
    }

    private String correctFormat(String key) {
        StringBuilder str = new StringBuilder();
        int count = 0;
        for (char c : key.toCharArray()) {
            if (count == 2) {
                count = 0;
                if (!Character.isWhitespace(c)) {
                    str.append(' ');
                    count = 1;
                }
                str.append(c);
            } else {
                str.append(c);
                count++;
            }
        }
        return str.toString();
    }

    public static abstract class Command {

        public abstract boolean execute(String[] cmdline, BlockingClient worker);

        public abstract String usage();

        public abstract String help();
    }
}
