package com.jd.ipc.forecasting.hdfs.archiver.service.impl;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.jd.ipc.forecasting.hdfs.archiver.service.IInputService;

public class NormalInputService implements IInputService {
	private FSDataInputStream ins;

	public static NormalInputService genarate(final FileStatus fs, final FileSystem fileSystem) throws IOException {
		NormalInputService service = new NormalInputService();
		service.ins = fileSystem.open(fs.getPath());
		return service;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return ins.read(b);
	}

}
