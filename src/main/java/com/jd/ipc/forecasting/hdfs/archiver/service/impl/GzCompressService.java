package com.jd.ipc.forecasting.hdfs.archiver.service.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;

import com.jd.ipc.forecasting.hdfs.archiver.service.IOutputService;

@Deprecated
public class GzCompressService implements IOutputService {
	Logger log = Logger.getLogger(getClass());
	private OutputStream cout;

	public static GzCompressService generate(String dstDir, String dstFileName, final FileSystem fileSystem) throws IOException {
		GzCompressService service = new GzCompressService();
		FSDataOutputStream fsout = fileSystem.create(new Path(dstDir + dstFileName + ".lzo"), true);

		DefaultCodec codec = new GzipCodec();
		codec.setConf(new Configuration());
		service.cout = codec.createOutputStream(fsout);
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
