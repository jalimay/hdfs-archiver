package com.jd.ipc.forecasting.hdfs.archiver;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.jd.ipc.forecasting.hdfs.archiver.service.IInputService;
import com.jd.ipc.forecasting.hdfs.archiver.service.IOutputService;
import com.jd.ipc.forecasting.hdfs.archiver.service.impl.CompressOutputService;
import com.jd.ipc.forecasting.hdfs.archiver.service.impl.NormalInputService;

public class Archiver {
	static Logger log = Logger.getLogger(Archiver.class);

	private static class ArchiveParam {
		String fsPath;
		String srcDir;
		String dstDir;
		String codeClass;
		int buffSize = 1024 * 1024;

		ArchiveParam(String[] args) {
			if (args.length == 1 && args[0].trim().equalsIgnoreCase("-h")) {
				System.out.println("Example: java -jar xxx.jar [BuffSize] [FsPath] [SrcDir] [DstDir] [CompressCode]");
				System.exit(1);
			} else if (args.length >= 4) {
				buffSize = Integer.valueOf(args[0]) * 1024 * 1024;
				fsPath = args[1].trim();
				srcDir = args[2].trim();
				dstDir = args[3].trim();
				if (args.length >= 5)
					codeClass = args[4].trim();
			} else {
				throw new RuntimeException("Not enough arguments");
			}
			validate();
		}

		private void validate() {
			if (buffSize == 0) {
				throw new RuntimeException("Buffer size is zero");
			}
			if (dstDir.charAt(dstDir.length() - 1) != '/') {
				dstDir = dstDir.concat("/");
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
		// read arguments and init params
		ArchiveParam ap = new ArchiveParam(args);
		log.info("FileSystem:" + ap.fsPath);
		log.info("Compress source:" + ap.srcDir);
		log.info("Compress destination:" + ap.dstDir);
		log.info("Compress code:" + ap.codeClass);
		log.info("Buffer size:" + ap.buffSize);

		// Communication with HDFS
		FileSystem fileSystem = FileSystem.get(URI.create(ap.fsPath), new Configuration());

		if (!fileSystem.isFile(new Path(ap.dstDir))) {
			Path src = new Path(ap.srcDir);
			IOutputService outputService = CompressOutputService.generate(ap.codeClass, ap.dstDir, src.getName(), fileSystem);

			if (!fileSystem.isFile(src)) {
				FileStatus[] fss = fileSystem.listStatus(src);
				for (FileStatus fs : fss) {
					if (!fs.isDir()) {
						readCompressAndWrite(fileSystem, fs, outputService, ap.buffSize);
					}
				}
			} else {
				readCompressAndWrite(fileSystem, fileSystem.getFileStatus(src), outputService, ap.buffSize);
			}
			outputService.close();
		}
		log.info("Compression end");
	}

	private static void readCompressAndWrite(FileSystem fileSystem, FileStatus fs, IOutputService outputService, int buffSize) throws IOException, InterruptedException {
		byte[] b = new byte[buffSize];
		IInputService inputService = NormalInputService.genarate(fs, fileSystem);
		long fileLength = fs.getLen();
		if (fileLength == 0)
			return;
		log.info("Compressing [" + fs.getPath().getName() + "]...");
		while (inputService.read(b) > 0) {
			outputService.write(b);
		}
	}
}
