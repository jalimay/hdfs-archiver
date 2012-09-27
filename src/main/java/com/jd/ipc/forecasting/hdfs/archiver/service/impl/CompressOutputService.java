package com.jd.ipc.forecasting.hdfs.archiver.service.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzopCodec;
import com.jd.ipc.forecasting.hdfs.archiver.service.IOutputService;

enum CompressCode {
	GZ, LZO, BZ2;
	static CompressionCodec getCodeInstance(String code) {
		if (code == null || code.isEmpty())
			return null;
		CompressionCodec codec = null;
		switch (CompressCode.valueOf(code.toUpperCase())) {
		case GZ:
			GzipCodec gzCodec = new GzipCodec();
			gzCodec.setConf(new Configuration());
			codec = gzCodec;
			break;
		case LZO:
			LzopCodec lzoCodec = new LzopCodec();
			lzoCodec.setConf(new Configuration());
			codec = lzoCodec;
			break;
		case BZ2:
			BZip2Codec bz2Codec = new BZip2Codec();
			codec = bz2Codec;
			break;
		default:
			throw new RuntimeException("Unsupported compression code");
		}
		return codec;
	}
}

public class CompressOutputService implements IOutputService {
	Logger log = Logger.getLogger(getClass());
	private OutputStream cout;

	public static CompressOutputService generate(final String compressCodeName, final String dstDir, final String dstFileName, final FileSystem fileSystem) throws IOException,
			InstantiationException, IllegalAccessException {
		CompressOutputService service = new CompressOutputService();
		FSDataOutputStream fsout = fileSystem.create(new Path(dstDir + dstFileName + (compressCodeName == null || compressCodeName.isEmpty() ? "" : "." + compressCodeName)), true);
		CompressionCodec instance = CompressCode.getCodeInstance(compressCodeName);
		if (instance != null) {
			service.cout = instance.createOutputStream(fsout);
		} else {
			service.cout = fsout;
		}
		return service;
	}

	@Override
	public void write(byte[] b) throws IOException, InterruptedException {
		cout.write(b);

	}

	@Override
	public void close() throws IOException {
		cout.close();
	}
}
