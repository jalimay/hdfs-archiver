package com.jd.ipc.forecasting.hdfs.archiver.service;

import java.io.IOException;

public interface IInputService {
	public int read(byte[] b) throws IOException;
}
