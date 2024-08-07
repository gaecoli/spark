/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.internal.io.cloud

import java.io.IOException

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputCommitterFactory, FileOutputFormat, PathOutputCommitter, PathOutputCommitterFactory}

import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, HadoopMapReduceCommitProtocol}

/**
 * Spark Commit protocol for Path Output Committers.
 * This committer will work with the `FileOutputCommitter` and subclasses.
 * All implementations *must* be serializable.
 *
 * Rather than ask the `FileOutputFormat` for a committer, it uses the
 * `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` factory
 * API to create the committer.
 *
 * In `setupCommitter` the factory is identified and instantiated;
 * this factory then creates the actual committer implementation.
 *
 * Dynamic Partition support will be determined once the committer is
 * instantiated in the setupJob/setupTask methods. If this
 * class was instantiated with `dynamicPartitionOverwrite` set to true,
 * then the instantiated committer must either be an instance of
 * `FileOutputCommitter` or it must implement the `StreamCapabilities`
 * interface and declare that it has the capability
 * `mapreduce.job.committer.dynamic.partitioning`.
 * That feature is available on Hadoop releases with the Intermediate
 * Manifest Committer for GCS and ABFS; it is not supported by the
 * S3A committers.
 * @constructor Instantiate.
 * @param jobId               job
 * @param dest                destination
 * @param stagingDirOverwrite does the caller want support for dynamic
 *                            partition overwrite?
 */
class PathOutputCommitProtocol(
    jobId: String,
    dest: String,
    stagingDirOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, dest, stagingDirOverwrite) with Serializable {

//  if (stagingDirOverwrite) {
//    // until there's explicit extensions to the PathOutputCommitProtocols
//    // to support the spark mechanism, it's left to the individual committer
//    // choice to handle partitioning.
//    throw new IOException(PathOutputCommitProtocol.UNSUPPORTED)
//  }

  /** The committer created. */
  @transient private var committer: PathOutputCommitter = _

  require(dest != null, "Null destination specified")

  private[cloud] val destination: String = dest

  /** The destination path. This is serializable in Hadoop 3. */
  private[cloud] val destPath: Path = new Path(destination)

  private val hasValidPath = Try { new Path(destination) }.isSuccess

  private var addedAbsPathFiles: mutable.Map[String, String] = null

  private var partitionPaths: mutable.Set[String] = null

  logTrace(s"Instantiated committer with job ID=$jobId;" +
    s" destination=$destPath;" +
    s" stagingDirOverwrite=$stagingDirOverwrite")

  import PathOutputCommitProtocol._

  /**
   * Set up the committer.
   * This creates it by talking directly to the Hadoop factories, instead
   * of the V1 `mapred.FileOutputFormat` methods.
   * @param context task attempt
   * @return the committer to use. This will always be a subclass of
   *         `PathOutputCommitter`.
   */
  override protected def setupCommitter(context: TaskAttemptContext): PathOutputCommitter = {
    logTrace(s"Setting up committer for path $destination")
    val factory = PathOutputCommitterFactory.getCommitterFactory(destPath, context.getConfiguration)
    committer =
      factory match {
        case fileOutputCommitterFactory: FileOutputCommitterFactory =>
          fileOutputCommitterFactory.createOutputCommitter(
            Option(FileOutputFormat.getOutputPath(context)).getOrElse(destPath),
            context)
        case _ =>
          factory.createOutputCommitter(destPath, context)
      }

    // Special feature to force out the FileOutputCommitter, so as to guarantee
    // that the binding is working properly.
    val rejectFileOutput = context.getConfiguration
      .getBoolean(REJECT_FILE_OUTPUT, REJECT_FILE_OUTPUT_DEFVAL)
    if (rejectFileOutput && committer.isInstanceOf[FileOutputCommitter]) {
      // the output format returned a file output format committer, which
      // is exactly what we do not want. So switch back to the factory.
      logTrace(s"Using committer factory $factory")
      committer = factory.createOutputCommitter(destPath, context)
    }

    logTrace(s"Using committer ${committer.getClass}")
    logTrace(s"Committer details: $committer")
    if (committer.isInstanceOf[FileOutputCommitter]) {
      require(!rejectFileOutput,
        s"Committer created is the FileOutputCommitter $committer")

      if (committer.isCommitJobRepeatable(context)) {
        // If FileOutputCommitter says its job commit is repeatable, it means
        // it is using the v2 algorithm, which is not safe for task commit
        // failures. Warn
        logTrace(s"Committer $committer may not be tolerant of task commit failures")
      }
    } else {
      // if required other committers need to be checked for dynamic partition
      // compatibility through a StreamCapabilities probe.
      if (stagingDirOverwrite) {
        if (supportsDynamicPartitions) {
          logDebug(
            s"Committer $committer has declared compatibility with dynamic partition overwrite")
        } else {
          throw new IOException(PathOutputCommitProtocol.UNSUPPORTED + ": " + committer)
        }
      }
    }
    committer
  }

  override def commitJob(jobContext: JobContext,
                         taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    if (isMagicS3Committer) {
      // If this is s3 magic, then we should remove old files before commit
      if (hasValidPath) {
        val (allAbsPathFiles, allPartitionPaths) =
          taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
        val hadoopConfiguration = jobContext.getConfiguration
        val fs = stagingDir.getFileSystem(hadoopConfiguration)
        val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
        val absParentPaths = filesToMove.values.map(new Path(_).getParent).toSet
        if (stagingDirOverwrite) {
          logDebug(s"Clean up absolute partition directories for overwriting: $absParentPaths")
          absParentPaths.foreach(path => path.getFileSystem(hadoopConfiguration).delete(path, true))
        }
        logDebug(s"Create absolute parent directories: $absParentPaths")
        absParentPaths.foreach(path => path.getFileSystem(hadoopConfiguration).mkdirs(path))

        val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
        logDebug(s"Clean up default partition directories for overwriting: $partitionPaths")
        if (stagingDirOverwrite) {
          for (part <- partitionPaths) {
            val finalPartPath = new Path(dest, part)
            if (!fs.delete(finalPartPath, true) && !fs.exists(finalPartPath.getParent)) {
              fs.mkdirs(finalPartPath.getParent)
            }
          }
        }
      }
      committer.commitJob(jobContext)
    } else {
      super.commitJob(jobContext, taskCommits)
    }
  }


  /**
   * Does the instantiated committer support dynamic partitions?
   * @return true if the committer declares itself compatible.
   */
  private def supportsDynamicPartitions = {
    committer.isInstanceOf[FileOutputCommitter] ||
      isMagicS3Committer ||
      (committer.isInstanceOf[StreamCapabilities] &&
        committer.asInstanceOf[StreamCapabilities]
          .hasCapability(CAPABILITY_DYNAMIC_PARTITIONING))
  }

  private def isMagicS3Committer: Boolean =
    committer.isInstanceOf[MagicS3GuardCommitter]

  /**
   * Create a temporary file for a task.
   *
   * @param taskContext task context
   * @param dir         optional subdirectory
   * @param spec        file naming specification
   * @return a path as a string
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {

    val workDir = committer.getWorkPath
    val parent = dir.map {
      d => new Path(workDir, d)
    }.getOrElse(workDir)
    if (stagingDirOverwrite) {
      assert(dir.isDefined,
        "The dataset to be written must be partitioned when stagingDirOverwrite is true.")
      partitionPaths += dir.get
    }
    val file = new Path(parent, getFilename(taskContext, spec))
    logInfo(s"Creating task file $file for dir $dir and spec $spec," +
      s" committer is: ${committer.getClass}, ${committer.getOutputPath}, ${committer.getWorkPath}")
    logTrace(s"Creating task file $file for dir $dir and spec $spec")
    file.toString
  }

  /**
   * Reject any requests for an absolute path file on a committer which
   * is not compatible with it.
   *
   * @param taskContext task context
   * @param absoluteDir final directory
   * @param spec output filename
   * @return a path string
   * @throws UnsupportedOperationException if incompatible
   */
  override def newTaskTempFileAbsPath(
    taskContext: TaskAttemptContext,
    absoluteDir: String,
    spec: FileNameSpec): String = {

    if (isMagicS3Committer) {
      val tmpAbsPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
      val absPath = addedAbsPathFiles(tmpAbsPath)
      if (!isS3APath(new Path(absPath))) {
        throw new RuntimeException(s"Magic s3a output: absolute output location " +
          s"not supported for $absPath")
      }
      absPath
    } else if (supportsDynamicPartitions) {
      super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    } else {
      throw new UnsupportedOperationException(s"Absolute output locations not supported" +
        s" by committer $committer")
    }
  }

  def isS3APath(path: Path): Boolean = {
    path.toUri.getScheme.equals("s3a")
  }
}

object PathOutputCommitProtocol {

  /**
   * Hadoop configuration option.
   * Fail fast if the committer is using the path output protocol.
   * This option can be used to catch configuration issues early.
   *
   * It's mostly relevant when testing/diagnostics, as it can be used to
   * enforce that schema-specific options are triggering a switch
   * to a new committer.
   */
  val REJECT_FILE_OUTPUT = "pathoutputcommit.reject.fileoutput"

  /**
   * Default behavior: accept the file output.
   */
  val REJECT_FILE_OUTPUT_DEFVAL = false

  /** Error string for tests. */
  private[cloud] val UNSUPPORTED: String = "PathOutputCommitter does not support" +
    " stagingDirOverwrite"

  /**
   * Stream Capabilities probe for spark dynamic partitioning compatibility.
   */
  private[cloud] val CAPABILITY_DYNAMIC_PARTITIONING =
    "mapreduce.job.committer.dynamic.partitioning"

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  private[cloud] val OUTPUTCOMMITTER_FACTORY_SCHEME = "mapreduce.outputcommitter.factory.scheme"
}
