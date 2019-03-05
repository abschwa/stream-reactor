/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSink}
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import org.scalatest.{DoNotDiscover, Matchers, WordSpec, PrivateMethodTester}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
@DoNotDiscover
class TestCassandraConnectionSecure extends WordSpec with Matchers with TestConfig with PrivateMethodTester {

  "should return a secured session" in {
    createKeySpace("connection", secure = true, ssl = false)
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> "localhost",
      CassandraConfigConstants.KEY_SPACE -> "connection",
      CassandraConfigConstants.USERNAME -> "cassandra",
      CassandraConfigConstants.PASSWD -> "cassandra",
      CassandraConfigConstants.KCQL -> "INSERT INTO TABLE SELECT * FROM TOPIC"
    ).asJava

    val taskConfig = CassandraConfigSink(props)
    val conn = CassandraConnection(taskConfig)
    val session = conn.session
    session should not be null
    session.getCluster.getConfiguration.getProtocolOptions.getAuthProvider should not be null

    val cluster = session.getCluster
    session.close()
    cluster.close()
  }

  "should return a DCAware cluster" in {
    createKeySpace("connection", secure = false, ssl = false)
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> "localhost",
      CassandraConfigConstants.KEY_SPACE -> "connection",
      CassandraConfigConstants.KCQL -> "INSERT INTO TABLE SELECT * FROM TOPIC",
      CassandraConfigConstants.DC_AWARE_ENABLED -> "true",
      CassandraConfigConstants.DC_AWARE_LOCAL_DC -> "localdc",
      CassandraConfigConstants.DC_AWARE_USED_HOSTS_PER_REMOTE_DC -> "1",
      CassandraConfigConstants.DC_AWARE_ALLOW_REMOTE_DCS_FOR_LOCAL_CONSISTENCY_LEVEL -> "true"
    ).asJava

    val taskConfig = CassandraConfigSink(props)
    val conn = CassandraConnection(taskConfig)
    val session = conn.session
    session should not be null

    val cluster = session.getCluster
    val loadBalancingPolicy = cluster.getConfiguration.getPolicies.getLoadBalancingPolicy
    loadBalancingPolicy should not be null
    loadBalancingPolicy.isInstanceOf[TokenAwarePolicy]
    val tokenAwarePolicy = loadBalancingPolicy.asInstanceOf[TokenAwarePolicy]
    val childPolicy = tokenAwarePolicy.getChildPolicy
    childPolicy should not be null
    childPolicy.isInstanceOf[DCAwareRoundRobinPolicy]
    val dc = PrivateMethod[DCAwareRoundRobinPolicy]('dc)
    assert("localdc" === (childPolicy invokePrivate dc(childPolicy)))
  }
}