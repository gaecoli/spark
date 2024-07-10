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
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, Watcher, WatcherException}
import io.fabric8.kubernetes.client.Watcher.Action
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.Logging

private[k8s] trait LoggingPodStatusWatcher extends Watcher[Pod] {
  def watchOrStop(submissionId: String): Boolean

  def reset(): Unit

  def withKubernetesClient(kubernetesClient: KubernetesClient): Unit
}

/**
 * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
 * every state change and also at an interval for liveness.
 *
 * @param conf kubernetes driver conf.
 */
private[k8s] class LoggingPodStatusWatcherImpl(conf: KubernetesDriverConf)
  extends LoggingPodStatusWatcher with Logging {
  private val rawLogger: Logger = LoggerFactory.getLogger("RawLog")

  private val appId = conf.appId

  private var podCompleted = false

  private var resourceTooOldReceived = false

  private var pod = Option.empty[Pod]

  private var kubernetesClient: Option[KubernetesClient] = None

  private val logExtractPattern = "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\.\\d{9}Z (.*)$".r

  private var lastSeenDriverLogTime: String = ""
  private var lastSeenDriverLogCnt: Int = 0

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("unknown")

  override def withKubernetesClient(kubernetesClient: KubernetesClient): Unit = {
    this.kubernetesClient = Option(kubernetesClient)
  }

  override def reset(): Unit = {
    resourceTooOldReceived = false
  }

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()

      case _ =>
        logLongStatus()
        if (hasCompleted()) {
          closeWatch()
        }
    }
  }

  override def onClose(e: WatcherException): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    if(e != null && e.isHttpGone) {
      resourceTooOldReceived = true
      logDebug(s"Got HTTP Gone code, resource version changed in k8s api: $e")
    } else {
      closeWatch()
    }
  }

  override def onClose(): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    closeWatch()
  }

  private def logLongStatus(): Unit = {
    logInfo("State changed, new state: " + pod.map(formatPodState).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = synchronized {
    podCompleted = true
    this.notifyAll()
  }

  private def logDriverLog(): Unit = {
    try {
      if (!conf.get(WAIT_FOR_APP_COMPLETION)) return
      if (phase == "Pending" || phase == "unknown") return
      if (pod.isEmpty || pod.exists(_.getStatus.getPhase == "")) return
      kubernetesClient.foreach { kClient =>
        if (pod.nonEmpty) {
          val sinceTime =
            if (lastSeenDriverLogTime.nonEmpty) s"${lastSeenDriverLogTime}Z" else null
          val logs = kClient.pods()
            .withName(pod.map(_.getMetadata.getName).get)
            .usingTimestamps()
            .sinceTime(sinceTime)
            .withPrettyOutput()
            .getLog()
          var lastSeenCnt = 0
          var lastSeenTime = lastSeenDriverLogTime
          logs.split("\n").foreach { line =>
            val logExtractPattern(ts, raw) = line
            if (ts == lastSeenTime) {
              lastSeenCnt += 1
            } else if (ts > lastSeenTime) {
              lastSeenTime = ts
              lastSeenCnt = 1
            }
            if ((ts == lastSeenDriverLogTime && lastSeenCnt > lastSeenDriverLogCnt) ||
              ts > lastSeenDriverLogTime
            ) {
              rawLogger.info(raw)
            }
          }
          lastSeenDriverLogTime = lastSeenTime.trim
          lastSeenDriverLogCnt = lastSeenCnt
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("failed to fetch driver logs...")
        if (e.getMessage.contains("unable to retrieve container logs for container")) {
          log.warn(s"container is terminate, can not fetch logs: ${e.getMessage}")
        } else {
          log.warn(e.getMessage, e)
        }
    }
  }

  override def watchOrStop(sId: String): Boolean = if (conf.get(WAIT_FOR_APP_COMPLETION)) {
    logInfo(s"Waiting for application ${conf.appName} with submission ID $sId to finish...")
    val interval = conf.get(REPORT_INTERVAL)
    synchronized {
      while (!podCompleted && !resourceTooOldReceived) {
        wait(interval)
        logInfo(s"Application status for $appId (phase: $phase)")
        logDriverLog()
      }
    }

    if (podCompleted) {
      logInfo(
        pod.map { p => s"Container final statuses:\n\n${containersDescription(p)}" }
          .getOrElse("No containers were found in the driver pod."))
      logInfo(s"Application ${conf.appName} with submission ID $sId finished")
    }
    podCompleted
  } else {
    logInfo(s"Deployed Spark application ${conf.appName} with submission ID $sId into Kubernetes")
    // Always act like the application has completed since we don't want to wait for app completion
    true
  }

}
