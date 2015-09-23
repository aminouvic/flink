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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Sink that emits its input elements to rolling
 * {@link org.apache.hadoop.fs.FileSystem} files. This is itegrated with the
 * checkpointing mechanism to provide exactly once semantics.
 *
 * <p>
 * When creating the sink a {@code basePath} must be specified. The base
 * directory contains one directory for every bucket. The bucket directories
 * themselves contain several part files. These contain the actual written data.
 *
 * <p>
 * The sink uses a {@link Bucketer} to determine the name of bucket directories
 * inside the base directory. Whenever the {@code Bucketer} returns a different
 * directory name than it returned before the sink will close the current part
 * files inside that bucket and start the new bucket directory. The default
 * bucketer is a {@link DateTimeBucketer} with date format string
 * {@code ""yyyy-MM-dd-HH"}. You can specify a custom {@code Bucketer} using
 * {@link #setBucketer(Bucketer)}. For example, use
 * {@link org.apache.flink.streaming.connectors.fs.NonRollingBucketer} if you
 * don't want to have buckets but still write part files in a fault-tolerant
 * way.
 *
 * <p>
 * The filenames of the part files contain the part prefix, the parallel subtask
 * index of the sink and a rolling counter, for example {@code "part-1-17"}. Per
 * default the part prefix is {@code "part"} but this can be configured using
 * {@link #setPartPrefix(String)}. When a part file becomes bigger than the
 * batch size the current part file is closed, the part counter is increased and
 * a new part file is created. The batch size defaults to {@code 384MB}, this
 * can be configured using {@link #setBatchSize(long)}.
 *
 * <p>
 * Part files can be in one of three states: in-progress, pending or finished.
 * The reason for this is how the sink works together with the checkpointing
 * mechanism to provide exactly-once semantics and fault-tolerance. The part
 * file that is currently being written to is in-progress. Once a part file is
 * closed for writing it becomes pending. When a checkpoint is successful the
 * currently pending files will be moved to finished. If a failure occurs the
 * pending files will be deleted to reset state to the last checkpoint. The data
 * in in-progress files will also have to be rolled back. If the
 * {@code FileSystem} supports the {@code truncate} call this will be used to
 * reset the file back to a previous state. If not, a special file with the same
 * name as the part file and the suffix {@code ".valid-length"} will be written
 * that contains the length up to which the file contains valid data. When
 * reading the file it must be ensured that it is only read up to that point.
 * The prefixes and suffixes for the different file states and valid-length
 * files can be configured, for example with {@link #setPendingSuffix(String)}.
 *
 * <p>
 * Note: If checkpointing is not enabled the pending files will never be moved
 * to the finished state. In that case, the pending suffix/prefix can be set to
 * {@code ""} to make the sink work in a non-fault-tolerant way but still
 * provide output without prefixes and suffixes.
 *
 * <p>
 * The part files are written using an instance of {@link Writer}. By default
 * {@link org.apache.flink.streaming.connectors.fs.StringWriter} is used, which
 * writes the result of {@code toString()} for every element. Separated by
 * newlines. You can configure the writer using {@link #setWriter(Writer)}. For
 * example, {@link org.apache.flink.streaming.connectors.fs.SequenceFileWriter}
 * can be used to write Hadoop {@code SequenceFiles}.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 *     new RollingSink<Tuple2<IntWritable, Text>>(outPath)
 *         .setWriter(new SequenceFileWriter<IntWritable, Text>())
 *         .setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm")
 * }</pre>
 *
 * This will create a sink that writes to {@code SequenceFiles} and rolls every
 * minute.
 *
 * @see org.apache.flink.streaming.connectors.fs.DateTimeBucketer
 * @see StringWriter
 * @see SequenceFileWriter
 *
 * @param <T> Type of the elements emitted by this sink
 */
public class RollingSink<T> extends RichSinkFunction<T> implements InputTypeConfigurable, Checkpointed<RollingSink.BucketState>, CheckpointNotifier {

	private static final long serialVersionUID = 1L;

	private static Logger LOG = LoggerFactory.getLogger(RollingSink.class);

	// --------------------------------------------------------------------------------------------
	//  User configuration values
	// --------------------------------------------------------------------------------------------
	// These are initialized with some defaults but are meant to be changeable by the user
	/**
	 * The default maximum size of part files.
	 *
	 * 6 times the default block size
	 */
	private final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;

	/**
	 * The default timeout before closing an idle part file (in minutes) -1 : no
	 * close timeout on idle files
	 */
	private final int DEFAULT_IDLE_FILE_TIMEOUT = -1;

	/**
	 * The maximum number of open files hold by the sink Used to avoid having
	 * many open files at once
	 */
	private final int DEFAULT_MAX_OPEN_FILES = 24;

	/**
	 * This is used for part files that we are writing to but which where not
	 * yet confirmed by a checkpoint.
	 */
	private final String DEFAULT_IN_PROGRESS_SUFFIX = ".in-progress";

	/**
	 * See above, but for prefix
	 */
	private final String DEFAULT_IN_PROGRESS_PREFIX = "_";

	/**
	 * This is used for part files that we are not writing to but which are not
	 * yet confirmed by checkpoint.
	 */
	private final String DEFAULT_PENDING_SUFFIX = ".pending";

	/**
	 * See above, but for prefix.
	 */
	private final String DEFAULT_PENDING_PREFIX = "_";

	/**
	 * When truncate() is not supported on the used FileSystem we instead write
	 * a file along the part file with this ending that contains the length up
	 * to which the part file is valid.
	 */
	private final String DEFAULT_VALID_SUFFIX = ".valid-length";

	/**
	 * See above, but for prefix.
	 */
	private final String DEFAULT_VALID_PREFIX = "_";

	/**
	 * The default prefix for part files.
	 */
	private final String DEFAULT_PART_REFIX = "part";

	/**
	 * The base {@code Path} that stored all rolling bucket directories.
	 */
	private final String basePath;

	/**
	 * The {@code Bucketer} that is used to determine the path of bucket
	 * directories.
	 */
	private Bucketer bucketer;

	/**
	 * We have a template and call duplicate() for each parallel writer in
	 * open() to get the actual writer that is used for the part files.
	 */
	private Writer<T> writerTemplate;

	/**
	 * Set of writers used for writing part files
	 */
	private Cache<Path, Writer> writers;

	/**
	 * Maximum size of part files. If files exceed this we close and create a
	 * new one in the same bucket directory.
	 */
	private long batchSize;

	/**
	 * The default timeout before closing an idle part file (in minutes) -1 : no
	 * close timeout on idle files
	 */
	private int idleTimeOut;

	/**
	 * The maximum number of open files hold by the sink Used to avoid having
	 * many open files at once
	 */
	private int maxOpenFiles;

	/**
	 * If this is true we remove any leftover in-progress/pending files when the
	 * sink is opened.
	 *
	 * <p>
	 * This should only be set to false if using the sink without checkpoints,
	 * to not remove the files already in the directory.
	 */
	private boolean cleanupOnOpen = true;

	// These are the actually configured prefixes/suffixes
	private String inProgressSuffix = DEFAULT_IN_PROGRESS_SUFFIX;
	private String inProgressPrefix = DEFAULT_IN_PROGRESS_PREFIX;

	private String pendingSuffix = DEFAULT_PENDING_SUFFIX;
	private String pendingPrefix = DEFAULT_PENDING_PREFIX;

	private String validLengthSuffix = DEFAULT_VALID_SUFFIX;
	private String validLengthPrefix = DEFAULT_VALID_PREFIX;

	private String partPrefix = DEFAULT_PART_REFIX;

	/**
	 * The part file that we are currently writing to.
	 */
	//private transient Path currentPartPath;
	// --------------------------------------------------------------------------------------------
	//  Internal fields (not configurable by user)
	// --------------------------------------------------------------------------------------------
	/**
	 * The {@code FSDataOutputStream} for the current part file.
	 */
	private transient FSDataOutputStream outStream;

	/**
	 * Our subtask index, retrieved from the {@code RuntimeContext} in
	 * {@link #open}.
	 */
	private transient int subtaskIndex;

	/**
	 * We use reflection to get the .truncate() method, this is only available
	 * starting with Hadoop 2.7
	 */
	private transient Method refTruncate;

	/**
	 * The state object that is handled by flink from snapshot/restore. In there
	 * we store the current part file path, the valid length of the in-progress
	 * files and pending part files.
	 */
	private transient BucketState bucketState;

	/**
	 * The Configuration Object used to access the file system
	 */
	private transient org.apache.hadoop.conf.Configuration fsConf;

	/**
	 * Creates a new {@code RollingSink} that writes files to the given base
	 * directory.
	 *
	 * <p>
	 * This uses a{@link DateTimeBucketer} as bucketer and a
	 * {@link StringWriter} has writer. The maximum bucket size is set to 384
	 * MB.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 * @param bucketer
	 * @param conf
	 */
	public RollingSink(String basePath, Bucketer bucketer, org.apache.hadoop.conf.Configuration conf) {
		this.basePath = basePath;
		if (bucketer != null) {
			this.bucketer = bucketer;
		} else {
			this.bucketer = new NonRollingBucketer();
		}
		this.batchSize = DEFAULT_BATCH_SIZE;
		this.idleTimeOut = DEFAULT_IDLE_FILE_TIMEOUT;
		this.maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
		this.writerTemplate = new StringWriter<>();
		if (conf != null) {
			this.fsConf = conf;
		} else {
			this.fsConf = new org.apache.hadoop.conf.Configuration();
		}
	}

	/**
	 *
	 * @param basePath
	 * @param bucketer
	 */
	public RollingSink(String basePath, Bucketer bucketer) {
		this(basePath, bucketer, null);
	}

	/**
	 * Creates an HDFS Sink using a non rolling bucketer
	 *
	 * @param basePath path to write to
	 */
	public RollingSink(String basePath) {
		this(basePath, null, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (this.writerTemplate instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) writerTemplate).setInputType(type, executionConfig);
		}
	}

	/**
	 * Initializes the writers LRU cache with the desired settings and the
	 * appropriate removal callback
	 *
	 * @return
	 */
	private Cache<Path, Writer> initWriters() {
		CacheBuilder<Path, Writer> res = CacheBuilder.newBuilder().maximumSize(maxOpenFiles).removalListener(new RemovalListener<Path, Writer>() {
			@Override
			public void onRemoval(RemovalNotification<Path, Writer> notification) {
				try {
					closeCurrentPartFile(notification.getValue());
				} catch (Exception ex) {
					LOG.error("Exception while removing writer!" + ex.getMessage());
				}
			}

		});
		if (idleTimeOut >= 0) {
			res = res.expireAfterAccess(idleTimeOut, TimeUnit.MINUTES);
		}
		return res.build();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

		if (bucketState == null) {
			bucketState = new BucketState();
		}

		writers = initWriters();

		if (fsConf == null) {
			fsConf = new org.apache.hadoop.conf.Configuration();
		}

		FileSystem fs = new Path(basePath).getFileSystem(fsConf);
		refTruncate = reflectTruncate(fs);

		// delete pending/in-progress files that might be left if we fail while
		// no checkpoint has yet been done
		try {
			if (fs.exists(new Path(basePath)) && cleanupOnOpen) {
				RemoteIterator<LocatedFileStatus> bucketFiles = fs.listFiles(new Path(basePath), true);

				while (bucketFiles.hasNext()) {
					LocatedFileStatus file = bucketFiles.next();
					if (file.getPath().toString().endsWith(pendingSuffix)) {
						// only delete files that contain our subtask index
						if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex)) {
							LOG.debug("Deleting leftover pending file {}", file.getPath().toString());
							fs.delete(file.getPath(), true);
						}
					}
					if (file.getPath().toString().endsWith(inProgressSuffix)) {
						// only delete files that contain our subtask index
						if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex)) {
							LOG.debug("Deleting leftover in-progress file {}", file.getPath().toString());
							fs.delete(file.getPath(), true);
						}
					}
				}
			}
		} catch (IOException e) {
			LOG.error("Error while deleting leftover pending/in-progress files: {}", e);
			throw new RuntimeException("Error while deleting leftover pending/in-progress files.", e);
		}
	}

	@Override
	public void close() throws Exception {
//		boolean interrupted = Thread.interrupted();
		for (Writer w : writers.asMap().values()) {
			closeCurrentPartFile(w);
		}

//		if (interrupted) {
//			Thread.currentThread().interrupt();
//		}
	}

	@Override
	public void invoke(T value) throws Exception {
		Writer writer = getWriter(value);
		writer.write(value);
	}

	/**
	 * Returns the writer to use for value
	 *
	 * @param value
	 * @return the {@code Writer} we should use for writing value
	 * @throws Exception
	 */
	private Writer getWriter(T value) throws Exception {
		Writer writer = null;
		Path newPathDirectory = bucketer.getBucketPath(new Path(basePath), value);
		synchronized (this) {
			writer = writers.getIfPresent(newPathDirectory);
			if (shouldRoll(writer)) {
				writer = openNewPartFile(writer, newPathDirectory);
			} else {
				// nothing return the current writer
			}
		}
		return writer;
	}

	/**
	 * Determines whether we should change the bucket file we are writing to.
	 *
	 * <p>
	 * This will roll if no file was created yet, if the file size is larger
	 * than the specified size or if the {@code Bucketer} determines that we
	 * should roll.
	 */
	private boolean shouldRoll(Writer writer) {
		boolean shouldRoll = false;
		if (writers.size() == 0) {
			shouldRoll = true;
			LOG.debug("RollingSink {} starting new initial bucket. ", subtaskIndex);
		} else {
			if (shouldStartNewBucket(writer)) {
				shouldRoll = true;
			}
		}

		return shouldRoll;
	}

	private boolean shouldStartNewBucket(Writer writer) {
		boolean shouldStart = false;
		if (writer != null) {
			long writePosition = writer.getValidFileLength();
			if (writePosition > batchSize) {
				shouldStart = true;
				LOG.debug(
						"RollingSink {} starting new bucket because file position {} is above batch size {}. ",
						subtaskIndex,
						writePosition,
						batchSize);
			}
		} else {
			shouldStart = true;
			LOG.debug("RollingSink {} starting new bucket because {} said we should. ",
					subtaskIndex,
					bucketer);
		}
		return shouldStart;
	}

	/**
	 * Opens a new part file.
	 *
	 * <p>
	 * This closes the old bucket file if @param writer is not null and opens a
	 * new bucket file into @param newBucketDirectory
	 *
	 */
	private Writer openNewPartFile(Writer writer, Path newBucketDirectory) throws Exception {

		int partCounter = 0;

		// invalidate current writer in order to trigger proper close of writer if not null
		if (writer != null) {
			Path p = writer.getFinalPath();
			if (p != null) {

				//Calling invalidate will trigger key deletion and calls closePartFile
				writers.invalidate(p.getParent());

				// in case we're opening a new part file in the same directory update parCounter to avoid useless check of fs exists later
				if (p.getParent().toString().equalsIgnoreCase(newBucketDirectory.toString())) {
					partCounter = writer.getPartNum() + 1;
				}
			}
		}

		FileSystem fs = new Path(basePath).getFileSystem(fsConf);

		try {
			if (!fs.exists(newBucketDirectory) && fs.mkdirs(newBucketDirectory)) {
				LOG.debug("Created new bucket directory: {}", newBucketDirectory);
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not create base path for new rolling file.", e);
		}

		//The fileName we wish to use
		Path finalPartPath = new Path(newBucketDirectory, partPrefix + "-" + subtaskIndex + "-" + partCounter);

		// This should work since there is only one parallel subtask that tries names with
		// our subtask id. Otherwise we would run into concurrency issues here.
		while (fs.exists(finalPartPath) || fs.exists(new Path(newBucketDirectory, pendingPrefix + finalPartPath.getName()).suffix(pendingSuffix))) {
			partCounter++;
			finalPartPath = new Path(newBucketDirectory, partPrefix + "-" + subtaskIndex + "-" + partCounter);
		}

		LOG.debug("Next part path is {}", finalPartPath.toString());

		Path inProgressPath = new Path(finalPartPath.getParent(), inProgressPrefix + finalPartPath.getName()).suffix(inProgressSuffix);

		outStream = fs.create(inProgressPath, false);

		if (writer == null) {
			writer = writerTemplate.duplicate();
		}

		// we pass the final name as active path name
		writer.open(outStream, new Path(finalPartPath.getParent(), finalPartPath.getName()), partCounter);

		// Add the new writer to the collection 
		writers.put(newBucketDirectory, writer);

		return writer;
	}

	/**
	 * Closes the current part file.
	 *
	 * <p>
	 * This moves the current in-progress part file to a pending file and adds
	 * it to the list of pending files in our bucket state.
	 */
	private void closeCurrentPartFile(Writer writer) throws Exception {

		Path currentPartPath = null;

		if (writer != null) {
			currentPartPath = writer.getFinalPath();
			writer.close();
		}

		if (currentPartPath != null) {
			Path inProgressPath = new Path(currentPartPath.getParent(), inProgressPrefix + currentPartPath.getName()).suffix(inProgressSuffix);
			Path pendingPath = new Path(currentPartPath.getParent(), pendingPrefix + currentPartPath.getName()).suffix(pendingSuffix);
			FileSystem fs = inProgressPath.getFileSystem(fsConf);
			fs.rename(inProgressPath, pendingPath);
			LOG.debug("Moving in-progress bucket {} to pending file {} ", inProgressPath, pendingPath);
			this.bucketState.pendingFiles.add(currentPartPath.toString());
		}
	}

	/**
	 * Gets the truncate() call using reflection.
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	private Method reflectTruncate(FileSystem fs) {
		Method m = null;
		if (fs != null) {
			Class<?> fsClass = fs.getClass();
			try {
				m = fsClass.getMethod("truncate", Path.class, long.class);
			} catch (NoSuchMethodException ex) {
				LOG.debug("Truncate not found. Will write a file with suffix '" + validLengthSuffix + "' "
						+ " and prefix " + validLengthPrefix + " to specify how many bytes in a bucket are valid.");
				return null;
			}

			// verify that truncate actually works
			FSDataOutputStream outputStream;
			Path testPath = new Path(UUID.randomUUID().toString());
			try {
				outputStream = fs.create(testPath);
				outputStream.writeUTF("hello");
				outputStream.close();
			} catch (IOException e) {
				LOG.error("Could not create file for checking if truncate works.", e);
				throw new RuntimeException("Could not create file for checking if truncate works.", e);
			}

			try {
				m.invoke(fs, testPath, 2);
			} catch (IllegalAccessException | InvocationTargetException e) {
				LOG.debug("Truncate is not supported.", e);
				m = null;
			}

			try {
				fs.delete(testPath, false);
			} catch (IOException e) {
				LOG.error("Could not delete truncate test file.", e);
				throw new RuntimeException("Could not delete truncate test file.", e);
			}
		}
		return m;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (bucketState.pendingFilesPerCheckpoint) {
			Set<Long> pastCheckpointIds = bucketState.pendingFilesPerCheckpoint.keySet();
			Set<Long> checkpointsToRemove = Sets.newHashSet();
			for (Long pastCheckpointId : pastCheckpointIds) {
				if (pastCheckpointId <= checkpointId) {
					LOG.debug("Moving pending files to final location for checkpoint {}",
							pastCheckpointId);
					// All the pending files are buckets that have been completed but are waiting to be renamed
					// to their final name
					for (String filename : bucketState.pendingFilesPerCheckpoint.get(
							pastCheckpointId)) {
						Path finalPath = new Path(filename);
						Path pendingPath = new Path(finalPath.getParent(),
								pendingPrefix + finalPath.getName()).suffix(pendingSuffix);

						FileSystem fs = pendingPath.getFileSystem(fsConf);
						fs.rename(pendingPath, finalPath);
						LOG.info(
								"Pending file {} has been moved to final location after complete checkpoint {}.",
								pendingPath,
								pastCheckpointId);

					}
					checkpointsToRemove.add(pastCheckpointId);
				}
			}
			for (Long toRemove : checkpointsToRemove) {
				bucketState.pendingFilesPerCheckpoint.remove(toRemove);
			}
		}
	}

	@Override
	public BucketState snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {

		for (Writer w : writers.asMap().values()) {
			w.flush();

			if (w.getFSDataOutputStream() != null) {
				String currentfile = w.getFinalPath().toString();
				long validlength = w.getValidFileLength();
				LOG.debug("snapshotState, current file: " + currentfile);
				bucketState.currentFiles.put(currentfile, validlength);
			}
		}

		synchronized (bucketState.pendingFilesPerCheckpoint) {
			bucketState.pendingFilesPerCheckpoint.put(checkpointId, bucketState.pendingFiles);
		}
		bucketState.pendingFiles = Lists.newArrayList();
		return bucketState;
	}

	@Override
	public void restoreState(BucketState state) {
		bucketState = state;
		// we can clean all the pending files since they where renamed to final files
		// after this checkpoint was successfull
		bucketState.pendingFiles.clear();
		FileSystem fs = null;

		try {
			fs = new Path(basePath).getFileSystem(fsConf);
		} catch (IOException e) {
			LOG.error("Error while creating FileSystem in checkpoint restore.", e);
			System.out.println("Error while creating FileSystem in checkpoint restore. " + e);
			throw new RuntimeException("Error while creating FileSystem in checkpoint restore.", e);
		}

		if (bucketState.currentFiles != null && !bucketState.currentFiles.isEmpty()) {

			List<String> fileNames = new ArrayList<>(bucketState.currentFiles.keySet());
			for (String currentFile : fileNames) {

				long currentFileValidLength = bucketState.currentFiles.get(currentFile);

				// We were writing to a file when the last checkpoint occured. This file can either
				// be still in-progress or became a pending file at some point after the checkpoint.
				// Either way, we have to truncate it back to a valid state (or write a .valid-length)
				// file that specifies up to which length it is valid and rename it to the final name
				// before starting a new bucket file.
				Path partPath = new Path(currentFile);
				try {
					Path partPendingPath = new Path(partPath.getParent(), pendingPrefix + partPath.getName()).suffix(
							pendingSuffix);
					Path partInProgressPath = new Path(partPath.getParent(), inProgressPrefix + partPath.getName()).suffix(inProgressSuffix);

					if (fs.exists(partPendingPath)) {
						LOG.debug("In-progress file {} has been moved to pending after checkpoint, moving to final location.", partPath);
						// has been moved to pending in the mean time, rename to final location
						fs.rename(partPendingPath, partPath);
					} else if (fs.exists(partInProgressPath)) {
						LOG.debug("In-progress file {} is still in-progress, moving to final location.", partPath);
						// it was still in progress, rename to final path
						fs.rename(partInProgressPath, partPath);
					} else {
						LOG.error("In-Progress file " + currentFile + " "
								+ "was neither moved to pending nor is still in progress.");
						throw new RuntimeException("In-Progress file " + currentFile + " "
								+ "was neither moved to pending nor is still in progress.");
					}

					refTruncate = reflectTruncate(fs);
					// truncate it or write a ".valid-length" file to specify up to which point it is valid
					if (refTruncate != null) {
						LOG.debug("Truncating {} to valid length {}", partPath, currentFileValidLength);
						refTruncate.invoke(fs, partPath, currentFileValidLength);
					} else {
						LOG.debug("Writing valid-length file for {} to specify valid length {}", partPath, currentFileValidLength);
						Path validLengthFilePath = new Path(partPath.getParent(), validLengthPrefix + partPath.getName()).suffix(validLengthSuffix);
						FSDataOutputStream lengthFileOut = fs.create(validLengthFilePath);
						lengthFileOut.writeUTF(Long.toString(currentFileValidLength));
						lengthFileOut.close();
					}

					// invalidate in the state object for current file
					bucketState.currentFiles.remove(currentFile);
				} catch (IOException e) {
					LOG.error("Error while restoring RollingSink state.", e);
					throw new RuntimeException("Error while restoring RollingSink state.", e);
				} catch (InvocationTargetException | IllegalAccessException e) {
					LOG.error("Cound not invoke truncate.", e);
					throw new RuntimeException("Could not invoke truncate.", e);
				}
			}
		}

		LOG.debug("Clearing pending/in-progress files.");

		// Move files that are confirmed by a checkpoint but did not get moved to final location
		// because the checkpoint notification did not happen before a failure
		Set<Long> pastCheckpointIds = bucketState.pendingFilesPerCheckpoint.keySet();
		LOG.debug("Moving pending files to final location on restore.");
		System.out.println("Moving pending files to final location for checkpoint.");
		for (Long pastCheckpointId : pastCheckpointIds) {
			// All the pending files are buckets that have been completed but are waiting to be renamed
			// to their final name
			for (String filename : bucketState.pendingFilesPerCheckpoint.get(pastCheckpointId)) {
				Path finalPath = new Path(filename);
				Path pendingPath = new Path(finalPath.getParent(), pendingPrefix + finalPath.getName()).suffix(pendingSuffix);

				try {
					if (fs.exists(pendingPath)) {
						LOG.debug(
								"Moving pending file {} to final location after complete checkpoint {}.",
								pendingPath,
								pastCheckpointId);
						System.out.println(
								"Moving pending file " + pendingPath + " to final location after complete checkpoint " + pastCheckpointId + ".");
						fs.rename(pendingPath, finalPath);
					}
				} catch (IOException e) {
					LOG.error("Error while renaming pending file {} to final path {}: {}", pendingPath, finalPath, e);
					System.out.println("Error while renaming pending file {} to final path {} " + pendingPath + " " + finalPath + " " + e);
					throw new RuntimeException("Error while renaming pending file " + pendingPath + " to final path " + finalPath, e);
				}
			}
		}

		bucketState.pendingFiles.clear();
		synchronized (bucketState.pendingFilesPerCheckpoint) {
			bucketState.pendingFilesPerCheckpoint.clear();
		}

		// we need to get this here since open() has not yet been called
		int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		// delete pending files
		try {
			RemoteIterator<LocatedFileStatus> bucketFiles = fs.listFiles(new Path(basePath), true);

			while (bucketFiles.hasNext()) {
				LocatedFileStatus file = bucketFiles.next();
				if (file.getPath().toString().endsWith(pendingSuffix)) {
					// only delete files that contain our subtask index
					if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex)) {
						LOG.debug("Deleting pending file {}", file.getPath().toString());
						fs.delete(file.getPath(), true);
					}
				}
				if (file.getPath().toString().endsWith(inProgressSuffix)) {
					// only delete files that contain our subtask index
					if (file.getPath().toString().contains(partPrefix + "-" + subtaskIndex)) {
						LOG.debug("Deleting in-progress file {}", file.getPath().toString());
						fs.delete(file.getPath(), true);
					}
				}
			}
		} catch (IOException e) {
			LOG.error("Error while deleting old pending files: {}", e);
			throw new RuntimeException("Error while deleting old pending files.", e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Setters for User configuration values
	// --------------------------------------------------------------------------------------------
	/**
	 * Sets the maximum bucket size in bytes.
	 *
	 * <p>
	 * When a bucket part file becomes larger than this size a new bucket part
	 * file is started and the old one is closed. The name of the bucket files
	 * depends on the {@link Bucketer}.
	 *
	 * @param batchSize The bucket part file size in bytes.
	 */
	public RollingSink<T> setBatchSize(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Sets the maximum number of simultanous open files
	 *
	 * <p>
	 * When the number of open files exceed's this param the least recently used
	 * file is closed before opening a new one
	 *
	 * @param maxOpenFiles the maximum number of simultanous open files
	 */
	public RollingSink<T> setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
		return this;
	}

	/**
	 * Sets the maximum amount of time we keep open an idle bucket
	 *
	 * <p>
	 * When a bucket part file becomes idle for an amount of time greater than
	 * this, the file is automatically closed.
	 *
	 * @param timeOut time out in minutes before closing an idle bucket file In
	 * number of minutes
	 */
	public RollingSink<T> setIdleTimeOut(int timeOut) {
		this.idleTimeOut = timeOut;
		return this;
	}

	/**
	 * Sets the {@link Bucketer} to use for determining the bucket files to
	 * write to.
	 *
	 * @param bucketer The bucketer to use.
	 */
	public RollingSink<T> setBucketer(Bucketer bucketer) {
		this.bucketer = bucketer;
		return this;
	}

	/**
	 * Sets the {@link Writer} to be used for writing the incoming elements to
	 * bucket files.
	 *
	 * @param writer The {@code Writer} to use.
	 */
	public RollingSink<T> setWriter(Writer<T> writer) {
		this.writerTemplate = writer;
		return this;
	}

	/**
	 * Sets the suffix of in-progress part files. The default is
	 * {@code "in-progress"}.
	 */
	public RollingSink<T> setInProgressSuffix(String inProgressSuffix) {
		this.inProgressSuffix = inProgressSuffix;
		return this;
	}

	/**
	 * Sets the prefix of in-progress part files. The default is {@code "_"}.
	 */
	public RollingSink<T> setInProgressPrefix(String inProgressPrefix) {
		this.inProgressPrefix = inProgressPrefix;
		return this;
	}

	/**
	 * Sets the suffix of pending part files. The default is {@code ".pending"}.
	 */
	public RollingSink<T> setPendingSuffix(String pendingSuffix) {
		this.pendingSuffix = pendingSuffix;
		return this;
	}

	/**
	 * Sets the prefix of pending part files. The default is {@code "_"}.
	 */
	public RollingSink<T> setPendingPrefix(String pendingPrefix) {
		this.pendingPrefix = pendingPrefix;
		return this;
	}

	/**
	 * Sets the suffix of valid-length files. The default is
	 * {@code ".valid-length"}.
	 */
	public RollingSink<T> setValidLengthSuffix(String validLengthSuffix) {
		this.validLengthSuffix = validLengthSuffix;
		return this;
	}

	/**
	 * Sets the prefix of valid-length files. The default is {@code "_"}.
	 */
	public RollingSink<T> setValidLengthPrefix(String validLengthPrefix) {
		this.validLengthPrefix = validLengthPrefix;
		return this;
	}

	/**
	 * Sets the prefix of part files. The default is {@code "part"}.
	 */
	public RollingSink<T> setPartPrefix(String partPrefix) {
		this.partPrefix = partPrefix;
		return this;
	}

	/**
	 * Disable cleanup of leftover in-progress/pending files when the sink is
	 * opened.
	 *
	 * <p>
	 * This should only be disabled if using the sink without checkpoints, to
	 * not remove the files already in the directory.
	 */
	public RollingSink<T> disableCleanupOnOpen() {
		this.cleanupOnOpen = false;
		return this;
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Classes
	// --------------------------------------------------------------------------------------------
	/**
	 * This is used for keeping track of the current in-progress files and files
	 * that we mark for moving from pending to final location after we get a
	 * checkpoint-complete notification.
	 */
	static final class BucketState implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * Map to keep list of files+valid_length that were in-progress when the
		 * last checkpoint occured
		 */
		Map<String, Long> currentFiles = Maps.newHashMap();

		/**
		 * Pending files that accumulated since the last checkpoint.
		 */
		List<String> pendingFiles = Lists.newArrayList();

		/**
		 * When doing a checkpoint we move the pending files since the last
		 * checkpoint to this map with the id of the checkpoint. When we get the
		 * checkpoint-complete notification we move pending files of completed
		 * checkpoints to their final location.
		 */
		final Map<Long, List<String>> pendingFilesPerCheckpoint = Maps.newHashMap();
	}
}
