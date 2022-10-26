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
package org.apache.spark.metrics

import java.io.File
import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import javax.management.ObjectName

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ProcfsMetricsGetter
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.MemoryManager

/**
 * Executor metric types for executor-level metrics stored in ExecutorMetrics.
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long]
  private[spark] def names: Seq[String]
}

sealed trait SingleValueExecutorMetricType extends ExecutorMetricType {
  override private[spark] def names = {
    Seq(getClass().getName().
      stripSuffix("$").split("""\.""").last)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](1)
    metrics(0) = getMetricValue(memoryManager)
    metrics
  }

  private[spark] def getMetricValue(memoryManager: MemoryManager): Long
}

private[spark] abstract class MemoryManagerExecutorMetricType(
    f: MemoryManager => Long) extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends SingleValueExecutorMetricType {
  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

case object ProcessTreeMetrics extends ExecutorMetricType {
  override val names = Seq(
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory")

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
    val processTreeMetrics = new Array[Long](names.length)
    processTreeMetrics(0) = allMetrics.jvmVmemTotal
    processTreeMetrics(1) = allMetrics.jvmRSSTotal
    processTreeMetrics(2) = allMetrics.pythonVmemTotal
    processTreeMetrics(3) = allMetrics.pythonRSSTotal
    processTreeMetrics(4) = allMetrics.otherVmemTotal
    processTreeMetrics(5) = allMetrics.otherRSSTotal
    processTreeMetrics
  }
}

case object GarbageCollectionMetrics extends ExecutorMetricType with Logging {
  private var nonBuiltInCollectors: Seq[String] = Nil

  override val names = Seq(
    "MinorGCCount",
    "MinorGCTime",
    "MajorGCCount",
    "MajorGCTime",
    "TotalGCTime"
  )

  /* We builtin some common GC collectors which categorized as young generation and old */
  private[spark] val YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "Copy",
    "PS Scavenge",
    "ParNew",
    "G1 Young Generation"
  )

  private[spark] val OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "MarkSweepCompact",
    "PS MarkSweep",
    "ConcurrentMarkSweep",
    "G1 Old Generation"
  )

  private lazy val youngGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS)
  }

  private lazy val oldGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val gcMetrics = new Array[Long](names.length)
    val mxBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    gcMetrics(4) = mxBeans.map(_.getCollectionTime).sum
    mxBeans.foreach { mxBean =>
      if (youngGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(0) = mxBean.getCollectionCount
        gcMetrics(1) = mxBean.getCollectionTime
      } else if (oldGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(2) = mxBean.getCollectionCount
        gcMetrics(3) = mxBean.getCollectionTime
      } else if (!nonBuiltInCollectors.contains(mxBean.getName)) {
        nonBuiltInCollectors = mxBean.getName +: nonBuiltInCollectors
        // log it when first seen
        logWarning(s"To enable non-built-in garbage collector(s) " +
          s"$nonBuiltInCollectors, users should configure it(them) to " +
          s"${config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS.key} or " +
          s"${config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS.key}")
      } else {
        // do nothing
      }
    }
    gcMetrics
  }
}

case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)

case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)

case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)

case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)

case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")

case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")

case object JVMMemoryPoolMetrics extends ExecutorMetricType {
  val prefix: String = "MemoryPool"
  val usedSuffix: String = "Used"
  val maxSuffix: String = "Max"
  override val names: Seq[String] = {
    Seq(
      "CodeCache", "Metaspace", "CompressedClassSpace", "PSEdenSpace", "PSSurvivorSpace", "PSOldGen"
    ).flatten(name => Seq(s"$prefix$name$usedSuffix", s"$prefix$name$maxSuffix"))
  }
  val name2Idx: Map[String, Int] = names.zipWithIndex.toMap

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val memoryPoolMetrics = new Array[Long](names.length)
    ManagementFactory.getMemoryPoolMXBeans.asScala
      .foreach { memoryPool =>
        val metricName = memoryPool.getName.replace(" ", "")
        val usage = memoryPool.getUsage
        name2Idx.get(s"$prefix$metricName$usedSuffix")
          .foreach(memoryPoolMetrics(_) = usage.getUsed)
        name2Idx.get(s"$prefix$metricName$maxSuffix")
          .foreach(memoryPoolMetrics(_) = usage.getMax)
      }
    memoryPoolMetrics
  }
}

case object JVMMemoryMetrics extends ExecutorMetricType {
  override private[spark] def names = Seq(
    "JVMMaxMemory",
    "JVMUsedMemory"
  )

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val memoryMetrics = new Array[Long](names.length)
    val rt = Runtime.getRuntime
    memoryMetrics(0) = rt.maxMemory()
    memoryMetrics(1) = rt.totalMemory() - rt.freeMemory()
    memoryMetrics
  }
}

case object PodMemoryMetrics extends ExecutorMetricType {
  override private[spark] def names = Seq(
    "PodMaxMemory",
    "PodRSSMemory",
    "PodCacheMemory",
    "PodUsedMemory"
  )

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val memoryStat = {
      try {
        val file = new File("/sys/fs/cgroup/memory/memory.stat")
        if (file.exists && file.canRead) {
          val source = Source.fromFile(file)
          val ret = source.getLines().toArray
          source.close()
          ret.toSeq
        } else Seq.empty
      } catch {
        case _: Throwable =>
          Seq.empty
      }
    }
      .flatMap {str =>
        val split = str.split(" ")
        if (split.length == 2) Some(split.head -> split.last.toLong)
        else None
      }.toMap

    val memoryMetrics = new Array[Long](names.length)
    memoryMetrics(0) = memoryStat.getOrElse("hierarchical_memory_limit", 0L)
    memoryMetrics(1) = memoryStat.getOrElse("total_rss", 0L)
    memoryMetrics(2) = memoryStat.getOrElse("total_cache", 0L)
    memoryMetrics(3) = memoryMetrics(1) + memoryMetrics(2)
    memoryMetrics
  }
}

private[spark] object ExecutorMetricType {

  // List of all executor metric getters
  val metricGetters = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory,
    ProcessTreeMetrics,
    GarbageCollectionMetrics,
    JVMMemoryPoolMetrics,
    JVMMemoryMetrics,
    PodMemoryMetrics
  )

  val (metricToOffset, numMetrics) = {
    var numberOfMetrics = 0
    val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
    metricGetters.foreach { m =>
      m.names.indices.foreach { idx =>
        definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
      }
      numberOfMetrics += m.names.length
    }
    (definedMetricsAndOffset, numberOfMetrics)
  }
}
