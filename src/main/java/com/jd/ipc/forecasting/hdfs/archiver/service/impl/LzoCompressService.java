package com.jd.ipc.forecasting.hdfs.archiver.service.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzopCodec;
import com.jd.ipc.forecasting.hdfs.archiver.service.IOutputService;

@Deprecated
public class LzoCompressService implements IOutputService {
	Logger log = Logger.getLogger(getClass());
	private OutputStream cout;

	public static LzoCompressService generate(String dstDir, String dstFileName, final FileSystem fileSystem) throws IOException {
		LzoCompressService service = new LzoCompressService();
		FSDataOutputStream fsout = fileSystem.create(new Path(dstDir + dstFileName + ".lzo"), true);
		LzopCodec lzopCodec = new LzopCodec();
		lzopCodec.setConf(new Configuration());
		service.cout = lzopCodec.createOutputStream(fsout);
		return service;
	}

	public void write(final byte[] b) throws IOException, InterruptedException {
		cout.write(b);
	}

	public void close() throws IOException {
		cout.close();
	}

}