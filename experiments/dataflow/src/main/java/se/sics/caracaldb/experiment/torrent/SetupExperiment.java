/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
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
package se.sics.caracaldb.experiment.torrent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SetupExperiment {

    final static int pieceSize = 1024;
    final static int piecesPerBlock = 1024;
    final static int blockSize = pieceSize * piecesPerBlock;
    final static long fileSize = 1024 * 1024 * 400 + 1024 * 100 + 100;

    public static void main(String[] args) throws IOException, HashUtil.HashBuilderException {
        String experimentDirPath = "./src/main/resources/torrent/";
        String uploadDirPath = experimentDirPath + "/uploader";
        File uploadDir = new File(uploadDirPath);
        if (!uploadDir.exists()) {
            uploadDir.mkdirs();
        } else {
            uploadDir.delete();
            uploadDir.mkdir();
        }

        String downloadDirPath = experimentDirPath + "/downloader";
        File downloadDir = new File(downloadDirPath);
        if (!downloadDir.exists()) {
            downloadDir.mkdir();
        } else {
            downloadDir.delete();
            downloadDir.mkdir();
        }

        File dataFile = new File(uploadDirPath + "/file");
        dataFile.createNewFile();
        Random rand = new Random(1234);
        generateFile(dataFile, rand);
        System.err.println("generated file size:" + fileSize + "/" + dataFile.length());

        if (!dataFile.exists()) {
            throw new RuntimeException("missing file");
        }
        File hashFile = new File(uploadDirPath + "/file.hash");
        hashFile.createNewFile();
        String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
        HashUtil.makeHashes(dataFile.getAbsolutePath(), hashFile.getAbsolutePath(), hashAlg, blockSize);
    }

    private static void generateFile(File file, Random rand) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        for (int i = 0; i < fileSize / 100; i++) {
            byte[] data = new byte[100];
            rand.nextBytes(data);
            out.write(data);
        }
        out.flush();
        out.close();
    }
}
