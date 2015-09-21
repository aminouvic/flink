/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flink.streaming.connectors.fs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@code Writer} is used in conjunction with a
 * {@link RollingSink} to perform the actual writing to the bucket files.
 *
 * All derived writers should implement this class to avoid reimplementing the
 * common functionalities 
 * 
 * @param <T> The type of the elements that are being written by the sink.
 */
public abstract class BaseWriter<T> implements Writer<T> {

	private static final long serialVersionUID = 1L;

	private static Logger LOG = LoggerFactory.getLogger(BaseWriter.class);

	/**
	 * The FSDataOutputStream used to access the fs by this writer
	 */
	protected transient FSDataOutputStream outputStream;

	/**
	 * The final file path this writer was opened for
	 */
	protected transient Path finalPath;

	/**
	 * The partnumber of the file we'rer writing
	 */
	protected transient int partNum = -1;

	/**
	 * We use reflection to get the hflush method or use sync as a fallback. The
	 * idea for this and the code comes from the Flume HDFS Sink.
	 */
	private transient Method refHflushOrSync;

	@Override
	public void open(FSDataOutputStream outStream, Path path, int part) throws IOException {
		if (outputStream != null) {
			throw new IllegalStateException("Writer has already been opened.");
		}
		this.outputStream = outStream;
		this.finalPath = path;
		this.partNum = part;

		// We do the reflection here since this is the first time that we have a FSDataOutputStream
		if (refHflushOrSync == null) {
			refHflushOrSync = reflectHflushOrSync(outStream);
		}

	}
	
	/**
	 * 
	 * @return the part number of the file we are currently writing
	 */
	@Override
	public int getPartNum() {
		return this.partNum;
	}

	@Override
	public void close() throws IOException {
		if (outputStream != null) {
			hflushOrSync(outputStream);
			outputStream.close();
		}
		finalPath = null;
		outputStream = null;
	}

	@Override
	public void flush() throws IOException {
		if (outputStream != null) {
			hflushOrSync(outputStream);
		}

	}

	@Override
	public FSDataOutputStream getFSDataOutputStream() {
		return this.outputStream;
	}

	@Override
	public Path getFinalPath() {
		return this.finalPath;
	}

	@Override
	public long getValidFileLength() {
		if (outputStream != null) {
			try {
				return outputStream.getPos();
			} catch (IOException ex) {
				return -1;
			}
		} else {
			return -1;
		}
	}

	/**
	 * If hflush is available in this version of HDFS, then this method calls
	 * hflush, else it calls sync.
	 *
	 * @param os - The stream to flush/sync
	 * @throws java.io.IOException
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	protected void hflushOrSync(FSDataOutputStream os) throws IOException {
		try {
			// At this point the refHflushOrSync cannot be null,
			// since register method would have thrown if it was.
			this.refHflushOrSync.invoke(os);
		} catch (InvocationTargetException e) {
			String msg = "Error while trying to hflushOrSync!";
			LOG.error(msg + " " + e.getCause());
			Throwable cause = e.getCause();
			if (cause != null && cause instanceof IOException) {
				throw (IOException) cause;
			}
			throw new RuntimeException(msg, e);
		} catch (Exception e) {
			String msg = "Error while trying to hflushOrSync!";
			LOG.error(msg + " " + e);
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * Gets the hflush call using reflection. Fallback to sync if hflush is not
	 * available.
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	private Method reflectHflushOrSync(FSDataOutputStream os) {
		Method m = null;
		if (os != null) {
			Class<?> fsDataOutputStreamClass = os.getClass();
			try {
				m = fsDataOutputStreamClass.getMethod("hflush");
			} catch (NoSuchMethodException ex) {
				LOG.debug("HFlush not found. Will use sync() instead");
				try {
					m = fsDataOutputStreamClass.getMethod("sync");
				} catch (Exception ex1) {
					String msg = "Neither hflush not sync were found. That seems to be "
							+ "a problem!";
					LOG.error(msg);
					throw new RuntimeException(msg, ex1);
				}
			}
		}
		return m;
	}
}