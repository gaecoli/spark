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

package org.apache.spark.sql.util

import org.apache.spark.annotation.Private
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

@Private
object FordealAuthUtils extends Logging {
  final private val ENV_EMAIL_KEY = "FORDEAL_USER_EMAIL";
  final private val USER_EMAIL_KEY = "fordeal.session.user";
  final private val SPARK_USER_EMAIL_KEY = s"spark.$USER_EMAIL_KEY";

  // get fordeal auth key
  def getAuthUser(sparkSession: SparkSession): String = {
    Option(
      Option(sparkSession.sparkContext.getLocalProperty(USER_EMAIL_KEY))
        .orElse(sparkSession.conf.getOption(SPARK_USER_EMAIL_KEY))
        .getOrElse(System.getenv(ENV_EMAIL_KEY))
    ).getOrElse("")
  }

  def getTableWithOwner(table: CatalogTable, sparkSession: SparkSession): CatalogTable = {
    log.info(s"=== table provider:${table.provider}, " +
      s" fordeal.session.user = " +
      s"${FordealAuthUtils.getAuthUser(sparkSession)}")

    table.copy(owner = FordealAuthUtils.getAuthUser(sparkSession))
  }

}
