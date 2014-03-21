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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.io.PrintWriter;
import jline.console.ConsoleReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.ResponseCode;
import util.log4j.ColoredPatternLayout;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Console {

    private ConsoleReader reader;
    private PrintWriter out;
    private Config conf;

    public static void main(String[] args) throws IOException {
        Console c = new Console();
        c.start();
    }

    public Console() throws IOException {
        reader = new ConsoleReader();
        out = new PrintWriter(reader.getOutput());
        PatternLayout layout = new ColoredPatternLayout("%d{[HH:mm:ss,SSS]} %-5p {%c{1}} %m%n");
        LogManager.getRootLogger().addAppender(new WriterAppender(layout, out));
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
            switch (cmd) {
                case "connect":
                    if (sline.length == 2) {
                        connect(sline[1]);
                    } else {
                        out.println("'connect' takes \"bootstrapaddress:port clientaddress:port\" parameters!");
                    }
                    break;
                case "config":
                    if (sline.length == 1) {
                        printConfig();
                    } else {
                        handleConfigCmd(sline[1]);
                    }
                    break;
                case "exit":
                case "quit":
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
            switch (cmd) {
                case "get":
                    if (cmdline.length == 2) {
                        Key k = Key.fromHex(correctFormat(cmdline[1]));
                        out.println("Getting " + k.toString() + "...");
                        GetResponse resp = worker.get(k);
                        if (resp.code == ResponseCode.SUCCESS) {
                            out.println("success!");
                            out.println("   " + k.toString() + "->" + new String(resp.data));
                        } else {
                            out.println("Result: " + resp.code.name());
                        }
                    } else {
                        out.println("Usage: get <key>");
                    }
                    break;
                case "put":
                    if (cmdline.length == 2) {
                        String[] kvline = cmdline[1].split(" ", 2);
                        if (kvline.length != 2) {
                            out.println("Usage: set <key> <value>");
                            continue;
                        }
                        Key k = Key.fromHex(correctFormat(kvline[0]));
                        byte[] value = kvline[1].getBytes();
                        out.println("Setting " + k.toString() + " to " + kvline[1] + "...");
                        ResponseCode resp = worker.put(k, value);
                        out.println("Result: " + resp.name());
                    } else {
                        out.println("Usage: set <key> <value>");
                    }
                    break;
                case "exit":
                case "quit":
                    out.println("Exiting...");
                    System.exit(0);
                default:
                    out.println("Unkown command: " + cmd);
            }
        }
    }

    private void handleConfigCmd(String line) {
        String[] sline = line.split(" ", 2);
        String cmd = sline[0];
        switch (cmd) {
            case "show":
            case "list":
                printConfig();
                break;
            case "get":
                if (sline.length == 2) {
                    out.println("   " + sline[1] + "->" + conf.getValue(sline[1]).render());
                } else {
                    out.println("Usage: config get <key>");
                }
                break;
            case "set":
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
                break;
            default:
                out.println("Usage: config show|list|get <key>|set <key> <value>");
        }
    }

    private void printConfig() {
        printConfig(conf);
    }

    private void printConfig(Config someConf) {
        someConf.entrySet().stream().forEach((e) -> {
            out.println("   " + e.getKey() + "->" + e.getValue().render());
        });
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
}
