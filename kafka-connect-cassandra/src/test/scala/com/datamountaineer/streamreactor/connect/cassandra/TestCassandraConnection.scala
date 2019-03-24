package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSink}
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import org.scalatest.{Matchers, PrivateMethodTester, WordSpec}

import scala.collection.JavaConverters._

class TestCassandraConnection extends WordSpec with Matchers with TestConfig with PrivateMethodTester{

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
