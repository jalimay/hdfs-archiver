package com.jd.ipc.forecasting.hdfs.archiver;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzopCodec;

public class Archiver {
	static Logger log = Logger.getLogger(Archiver.class);

	private static class ArchiveParam {
		String fsPath;
		String srcDir;
		String dstDir;
		int buffSize = 1024 * 1024;

		ArchiveParam(String[] args) {
			if (args.length == 1 && args[0].trim().equalsIgnoreCase("-h")) {
				System.out.println("Example: java -jar xxx.jar [BuffSize] [FsPath] [SrcDir] [DstDir]");
				System.exit(1);
			} else if (args.length >= 4) {
				buffSize = Integer.valueOf(args[0]) * 1024 * 1024;
				fsPath = args[1];
				srcDir = args[2];
				dstDir = args[3];
			} else {
				throw new RuntimeException("Not enough arguments");
			}
			validate();
		}

		private void validate() {
			if (buffSize == 0) {
				throw new RuntimeException("Buffer size is zero");
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// read arguments and init params
		ArchiveParam ap = new ArchiveParam(args);
		log.info("FileSystem:" + ap.fsPath);
		log.info("Compress source:" + ap.srcDir);
		log.info("Compress destination:" + ap.dstDir);
		log.info("Buffer size:" + ap.buffSize);

		// Communication with HDFS
		FileSystem fileSystem = FileSystem.get(URI.create(ap.fsPath), new Configuration());

		if (!fileSystem.isFile(new Path(ap.dstDir))) {
			Path src = new Path(ap.srcDir);
			CompressService compressService = new LzopCompressService(ap.buffSize);
			OutputStream cout = compressService.getOutputStream(ap.dstDir, src.getName(), fileSystem);
			if (!fileSystem.isFile(src)) {
				FileStatus[] fss = fileSystem.listStatus(src);
				for (FileStatus fs : fss) {
					if (!fs.isDir()) {
						compressService.compress(fs, fileSystem, cout);
					}
				}
			} else {
				compressService.compress(fileSystem.getFileStatus(src), fileSystem, cout);
			}
			cout.close();
		}
		log.info("Compression end");
	}
}

interface CompressService {
	/**
	 * 在FileSystem中创建dstDir/dstName的输入文件
	 * 
	 * @param dstDir
	 * @param dstName
	 * @param fileSystem
	 * @return
	 * @throws IOException
	 */
	public OutputStream getOutputStream(String dstDir, String dstName, final FileSystem fileSystem) throws IOException;

	/**
	 * 从FileSystem中读取FileStatus的所有内容，并压缩后输出到OutputStream中
	 * 
	 * @param fs
	 * @param fileSystem
	 * @param cout
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void compress(final FileStatus fs, final FileSystem fileSystem, final OutputStream cout) throws IOException, InterruptedException;
}

class LzopCompressService implements CompressService {
	Logger log = Logger.getLogger(getClass());
	private final int bufferSize;

	public LzopCompressService(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public OutputStream getOutputStream(String dstDir, String dstName, final FileSystem fileSystem) throws IOException {
		FSDataOutputStream fsout = fileSystem.create(new Path(dstDir + dstName + ".lzo"), true);
		LzopCodec lzopCodec = new LzopCodec();
		lzopCodec.setConf(new Configuration());
		OutputStream cout = lzopCodec.createOutputStream(fsout);
		return cout;
	}

	public void compress(final FileStatus fs, final FileSystem fileSystem, final OutputStream cout) throws IOException, InterruptedException {
		byte[] b = new byte[bufferSize];
		log.info("Compressing [" + fs.getPath().getName() + "]...");
		long fileLength = fs.getLen();
		if (fileLength == 0)
			return;
		long bt, et;
		bt = new Date().getTime();
		FSDataInputStream ins = fileSystem.open(fs.getPath());
		while (ins.read(b) > 0) {
			cout.write(b);
			log.info(Math.round(100 * ((double) ins.getPos() / fileLength)) + "%");
			Thread.sleep(100);
		}
		ins.close();
		cout.flush();
		et = new Date().getTime();

		log.info("Compress [" + fs.getPath().getName() + "] cost:" + (et - bt) / 1000 + " sec");
	}

}