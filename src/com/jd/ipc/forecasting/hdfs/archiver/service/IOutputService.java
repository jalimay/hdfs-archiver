package com.jd.ipc.forecasting.hdfs.archiver.service;

import java.io.IOException;

public interface IOutputService {

	/**
	 * 从FileSystem中读取FileStatus的所有内容，并压缩后输出到OutputStream中
	 * 
	 * @param fs
	 * @param fileSystem
	 * @param cout
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void write(final byte[] b) throws IOException, InterruptedException;

	public void close() throws IOException;
}