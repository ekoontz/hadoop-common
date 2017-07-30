/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ha;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestZKFailoverController extends ClientBaseWithFixes {
  private Configuration conf;
  private MiniZKFCCluster cluster;
  
  // Set up ZK digest-based credentials for the purposes of the tests,
  // to make sure all of our functionality works with auth and ACLs
  // present.
  private static final String DIGEST_USER_PASS="test-user:test-password";
  private static final String TEST_AUTH_GOOD =
    "digest:" + DIGEST_USER_PASS;
  private static final String DIGEST_USER_HASH;
  static {
    try {
      DIGEST_USER_HASH = DigestAuthenticationProvider.generateDigest(
          DIGEST_USER_PASS);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
  private static final String TEST_ACL =
    "digest:" + DIGEST_USER_HASH + ":rwcda";
  
  static {
    ((Log4JLogger)ActiveStandbyElector.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @Before
  public void setupConfAndServices() {
    // skip tests on Windows until after resolution of ZooKeeper client bug
    assumeTrue(!Shell.WINDOWS);
    conf = new Configuration();
    conf.set(ZKFailoverController.ZK_ACL_KEY, TEST_ACL);
    conf.set(ZKFailoverController.ZK_AUTH_KEY, TEST_AUTH_GOOD);

    conf.set(ZKFailoverController.ZK_QUORUM_KEY, hostPort);
    this.cluster = new MiniZKFCCluster(conf, getServer(serverFactory));
  }

  /**
   * Test that the various command lines for formatting the ZK directory
   * function correctly.
   */
  @Test(timeout=15000)
  public void testFormatZK() throws Exception {
    DummyHAService svc = cluster.getService(1);
    // Run without formatting the base dir,
    // should barf
    assertEquals(ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE,
        runFC(svc));

    // Format the base dir, should succeed
    assertEquals(0, runFC(svc, "-formatZK"));

    // Should fail to format if already formatted
    assertEquals(ZKFailoverController.ERR_CODE_FORMAT_DENIED,
        runFC(svc, "-formatZK", "-nonInteractive"));
  
    // Unless '-force' is on
    assertEquals(0, runFC(svc, "-formatZK", "-force"));
  }
  
  /**
   * Test that if ZooKeeper is not running, the correct error
   * code is returned.
   */
  @Test(timeout=15000)
  public void testNoZK() throws Exception {
    stopServer();
    DummyHAService svc = cluster.getService(1);
    assertEquals(ZKFailoverController.ERR_CODE_NO_ZK,
        runFC(svc));
  }
  
  @Test
  public void testFormatOneClusterLeavesOtherClustersAlone() throws Exception {
    DummyHAService svc = cluster.getService(1);

    DummyZKFC zkfcInOtherCluster = new DummyZKFC(conf, cluster.getService(1)) {
      @Override
      protected String getScopeInsideParentNode() {
        return "other-scope";
      }
    };
    
    // Run without formatting the base dir,
    // should barf
    assertEquals(ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE,
        runFC(svc));

    // Format the base dir, should succeed
    assertEquals(0, runFC(svc, "-formatZK"));
    
    // Run the other cluster without formatting, should barf because
    // it uses a different parent znode
    assertEquals(ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE,
        zkfcInOtherCluster.run(new String[]{}));
    
    // Should succeed in formatting the second cluster
    assertEquals(0, zkfcInOtherCluster.run(new String[]{"-formatZK"}));

    // But should not have deleted the original base node from the first
    // cluster
    assertEquals(ZKFailoverController.ERR_CODE_FORMAT_DENIED,
        runFC(svc, "-formatZK", "-nonInteractive"));
  }
  
  /**
   * Test that automatic failover won't run against a target that hasn't
   * explicitly enabled the feature.
   */
  @Test(timeout=10000)
  public void testWontRunWhenAutoFailoverDisabled() throws Exception {
    DummyHAService svc = cluster.getService(1);
    svc = Mockito.spy(svc);
    Mockito.doReturn(false).when(svc).isAutoFailoverEnabled();
    
    assertEquals(ZKFailoverController.ERR_CODE_AUTO_FAILOVER_NOT_ENABLED,
        runFC(svc, "-formatZK"));
    assertEquals(ZKFailoverController.ERR_CODE_AUTO_FAILOVER_NOT_ENABLED,
        runFC(svc));
  }
  
  /**
   * Test that, if ACLs are specified in the configuration, that
   * it sets the ACLs when formatting the parent node.
   */
  @Test(timeout=15000)
  public void testFormatSetsAcls() throws Exception {
    // Format the base dir, should succeed
    DummyHAService svc = cluster.getService(1);
    assertEquals(0, runFC(svc, "-formatZK"));

    ZooKeeper otherClient = createClient();
    try {
      // client without auth should not be able to read it
      Stat stat = new Stat();
      otherClient.getData(ZKFailoverController.ZK_PARENT_ZNODE_DEFAULT,
          false, stat);
      fail("Was able to read data without authenticating!");
    } catch (KeeperException.NoAuthException nae) {
      // expected
    }
  }
  
  /**
   * Test that the ZKFC won't run if fencing is not configured for the
   * local service.
   */
  @Test(timeout=15000)
  public void testFencingMustBeConfigured() throws Exception {
    DummyHAService svc = Mockito.spy(cluster.getService(0));
    Mockito.doThrow(new BadFencingConfigurationException("no fencing"))
        .when(svc).checkFencingConfigured();
    // Format the base dir, should succeed
    assertEquals(0, runFC(svc, "-formatZK"));
    // Try to run the actual FC, should fail without a fencer
    assertEquals(ZKFailoverController.ERR_CODE_NO_FENCER,
        runFC(svc));
  }
  
  /**
   * Test that, when the health monitor indicates bad health status,
   * failover is triggered. Also ensures that graceful active->standby
   * transition is used when possible, falling back to fencing when
   * the graceful approach fails.
   */
  @Test(timeout=15000)
  public void testAutoFailoverOnBadHealth() throws Exception {
    try {
      cluster.start();
      DummyHAService svc1 = cluster.getService(1);
      
      LOG.info("Faking svc0 unhealthy, should failover to svc1");
      cluster.setHealthy(0, false);
      
      LOG.info("Waiting for svc0 to enter standby state");
      cluster.waitForHAState(0, HAServiceState.STANDBY);
      cluster.waitForHAState(1, HAServiceState.ACTIVE);
  
      LOG.info("Allowing svc0 to be healthy again, making svc1 unreachable " +
          "and fail to gracefully go to standby");
      cluster.setUnreachable(1, true);
      cluster.setHealthy(0, true);
 
      // Should fail back to svc0 at this point
      cluster.waitForHAState(0, HAServiceState.ACTIVE);
      // and fence svc1
      Mockito.verify(svc1.fencer).fence(Mockito.same(svc1));
    } finally {
      cluster.stop();
    }
  }
  
  @Test(timeout=15000)
  public void testAutoFailoverOnLostZKSession() throws Exception {
    try {
      cluster.start();

      // Expire svc0, it should fail over to svc1
      cluster.expireAndVerifyFailover(0, 1);
      
      // Expire svc1, it should fail back to svc0
      cluster.expireAndVerifyFailover(1, 0);
      
      LOG.info("======= Running test cases second time to test " +
          "re-establishment =========");
      // Expire svc0, it should fail over to svc1
      cluster.expireAndVerifyFailover(0, 1);
      
      // Expire svc1, it should fail back to svc0
      cluster.expireAndVerifyFailover(1, 0);
    } finally {
      cluster.stop();
    }
  }

  /**
   * Test that, if the standby node is unhealthy, it doesn't try to become
   * active
   */
  @Test(timeout=15000)
  public void testDontFailoverToUnhealthyNode() throws Exception {
    try {
      cluster.start();

      // Make svc1 unhealthy, and wait for its FC to notice the bad health.
      cluster.setHealthy(1, false);
      cluster.waitForHealthState(1, HealthMonitor.State.SERVICE_UNHEALTHY);
      
      // Expire svc0
      cluster.getElector(0).preventSessionReestablishmentForTests();
      try {
        cluster.expireActiveLockHolder(0);

        LOG.info("Expired svc0's ZK session. Waiting a second to give svc1" +
            " a chance to take the lock, if it is ever going to.");
        Thread.sleep(1000);
        
        // Ensure that no one holds the lock.
        cluster.waitForActiveLockHolder(null);
        
      } finally {
        LOG.info("Allowing svc0's elector to re-establish its connection");
        cluster.getElector(0).allowSessionReestablishmentForTests();
      }
      // svc0 should get the lock again
      cluster.waitForActiveLockHolder(0);
    } finally {
      cluster.stop();
    }
  }

  /**
   * Test that the ZKFC successfully quits the election when it fails to
   * become active. This allows the old node to successfully fail back.
   */
  @Test(timeout=15000)
  public void testBecomingActiveFails() throws Exception {
    try {
      cluster.start();
      DummyHAService svc1 = cluster.getService(1);
      
      LOG.info("Making svc1 fail to become active");
      cluster.setFailToBecomeActive(1, true);
      
      LOG.info("Faking svc0 unhealthy, should NOT successfully " +
          "failover to svc1");
      cluster.setHealthy(0, false);
      cluster.waitForHealthState(0, State.SERVICE_UNHEALTHY);
      cluster.waitForActiveLockHolder(null);

      
      Mockito.verify(svc1.proxy, Mockito.timeout(2000).atLeastOnce())
        .transitionToActive(Mockito.<StateChangeRequestInfo>any());

      cluster.waitForHAState(0, HAServiceState.STANDBY);
      cluster.waitForHAState(1, HAServiceState.STANDBY);
      
      LOG.info("Faking svc0 healthy again, should go back to svc0");
      cluster.setHealthy(0, true);
      cluster.waitForHAState(0, HAServiceState.ACTIVE);
      cluster.waitForHAState(1, HAServiceState.STANDBY);
      cluster.waitForActiveLockHolder(0);
      
      // Ensure that we can fail back to svc1  once it it is able
      // to become active (e.g the admin has restarted it)
      LOG.info("Allowing svc1 to become active, expiring svc0");
      svc1.failToBecomeActive = false;
      cluster.expireAndVerifyFailover(0, 1);
    } finally {
      cluster.stop();
    }
  }
  
  /**
   * Test that, when ZooKeeper fails, the system remains in its
   * current state, without triggering any failovers, and without
   * causing the active node to enter standby state.
   */
  @Test(timeout=15000)
  public void testZooKeeperFailure() throws Exception {
    try {
      cluster.start();

      // Record initial ZK sessions
      long session0 = cluster.getElector(0).getZKSessionIdForTests();
      long session1 = cluster.getElector(1).getZKSessionIdForTests();

      LOG.info("====== Stopping ZK server");
      stopServer();
      waitForServerDown(hostPort, CONNECTION_TIMEOUT);
      
      LOG.info("====== Waiting for services to enter NEUTRAL mode");
      cluster.waitForElectorState(0,
          ActiveStandbyElector.State.NEUTRAL);
      cluster.waitForElectorState(1,
          ActiveStandbyElector.State.NEUTRAL);

      LOG.info("====== Checking that the services didn't change HA state");
      assertEquals(HAServiceState.ACTIVE, cluster.getService(0).state);
      assertEquals(HAServiceState.STANDBY, cluster.getService(1).state);
      
      LOG.info("====== Restarting server");
      startServer();
      waitForServerUp(hostPort, CONNECTION_TIMEOUT);

      // Nodes should go back to their original states, since they re-obtain
      // the same sessions.
      cluster.waitForElectorState(0, ActiveStandbyElector.State.ACTIVE);
      cluster.waitForElectorState(1, ActiveStandbyElector.State.STANDBY);
      // Check HA states didn't change.
      cluster.waitForHAState(0, HAServiceState.ACTIVE);
      cluster.waitForHAState(1, HAServiceState.STANDBY);

      // Check they re-used the same sessions and didn't spuriously reconnect
      assertEquals(session0,
          cluster.getElector(0).getZKSessionIdForTests());
      assertEquals(session1,
          cluster.getElector(1).getZKSessionIdForTests());
    } finally {
      cluster.stop();
    }
  }
  
  /**
   * Test that the ZKFC can gracefully cede its active status.
   */
  @Test(timeout=15000)
  public void testCedeActive() throws Exception {
    try {
      cluster.start();
      DummyZKFC zkfc = cluster.getZkfc(0);
      // It should be in active to start.
      assertEquals(ActiveStandbyElector.State.ACTIVE,
          zkfc.getElectorForTests().getStateForTests());

      // Ask it to cede active for 3 seconds. It should respond promptly
      // (i.e. the RPC itself should not take 3 seconds!)
      ZKFCProtocol proxy = zkfc.getLocalTarget().getZKFCProxy(conf, 5000);
      long st = Time.now();
      proxy.cedeActive(3000);
      long et = Time.now();
      assertTrue("RPC to cedeActive took " + (et - st) + " ms",
          et - st < 1000);
      
      // Should be in "INIT" state since it's not in the election
      // at this point.
      assertEquals(ActiveStandbyElector.State.INIT,
          zkfc.getElectorForTests().getStateForTests());

      // After the prescribed 3 seconds, should go into STANDBY state,
      // since the other node in the cluster would have taken ACTIVE.
      cluster.waitForElectorState(0, ActiveStandbyElector.State.STANDBY);
      long et2 = Time.now();
      assertTrue("Should take ~3 seconds to rejoin. Only took " + (et2 - et) +
          "ms before rejoining.",
          et2 - et > 2800);      
    } finally {
      cluster.stop();
    }
  }
  
  @Test(timeout=25000)
  public void testGracefulFailover() throws Exception {
    try {
      cluster.start();

      cluster.waitForActiveLockHolder(0);
      cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
      cluster.waitForActiveLockHolder(1);

      cluster.getService(0).getZKFCProxy(conf, 5000).gracefulFailover();
      cluster.waitForActiveLockHolder(0);

      Thread.sleep(10000); // allow to quiesce

      assertEquals(0, cluster.getService(0).fenceCount);
      assertEquals(0, cluster.getService(1).fenceCount);
      assertEquals(2, cluster.getService(0).activeTransitionCount);
      assertEquals(1, cluster.getService(1).activeTransitionCount);
    } finally {
      cluster.stop();
    }
  }
  
  @Test(timeout=15000)
  public void testGracefulFailoverToUnhealthy() throws Exception {
    try {
      cluster.start();

      cluster.waitForActiveLockHolder(0);

      // Mark it unhealthy, wait for it to exit election
      cluster.setHealthy(1, false);
      cluster.waitForElectorState(1, ActiveStandbyElector.State.INIT);
      
      // Ask for failover, it should fail, because it's unhealthy
      try {
        cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
        fail("Did not fail to graceful failover to unhealthy service!");
      } catch (ServiceFailedException sfe) {
        GenericTestUtils.assertExceptionContains(
            cluster.getService(1).toString() + 
            " is not currently healthy.", sfe);
      }
    } finally {
      cluster.stop();
    }
  }
  
  @Test(timeout=15000)
  public void testGracefulFailoverFailBecomingActive() throws Exception {
    try {
      cluster.start();

      cluster.waitForActiveLockHolder(0);
      cluster.setFailToBecomeActive(1, true);
      
      // Ask for failover, it should fail and report back to user.
      try {
        cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
        fail("Did not fail to graceful failover when target failed " +
            "to become active!");
      } catch (ServiceFailedException sfe) {
        GenericTestUtils.assertExceptionContains(
            "Couldn't make " + cluster.getService(1) + " active", sfe);
        GenericTestUtils.assertExceptionContains(
            "injected failure", sfe);
      }
      
      // No fencing
      assertEquals(0, cluster.getService(0).fenceCount);
      assertEquals(0, cluster.getService(1).fenceCount);

      // Service 0 should go back to being active after the failed failover
      cluster.waitForActiveLockHolder(0);
    } finally {
      cluster.stop();
    }
  }

  @Test(timeout=15000)
  public void testGracefulFailoverFailBecomingStandby() throws Exception {
    try {
      cluster.start();

      cluster.waitForActiveLockHolder(0);
      
      // Ask for failover when old node fails to transition to standby.
      // This should trigger fencing, since the cedeActive() command
      // still works, but leaves the breadcrumb in place.
      cluster.setFailToBecomeStandby(0, true);
      cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();

      // Check that the old node was fenced
      assertEquals(1, cluster.getService(0).fenceCount);
    } finally {
      cluster.stop();
    }
  }
  
  @Test(timeout=15000)
  public void testGracefulFailoverFailBecomingStandbyAndFailFence()
      throws Exception {
    try {
      cluster.start();

      cluster.waitForActiveLockHolder(0);
      
      // Ask for failover when old node fails to transition to standby.
      // This should trigger fencing, since the cedeActive() command
      // still works, but leaves the breadcrumb in place.
      cluster.setFailToBecomeStandby(0, true);
      cluster.setFailToFence(0, true);

      try {
        cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
        fail("Failover should have failed when old node wont fence");
      } catch (ServiceFailedException sfe) {
        GenericTestUtils.assertExceptionContains(
            "Unable to fence " + cluster.getService(0), sfe);
      }
    } finally {
      cluster.stop();
    }
  }

  /**
   * Test which exercises all of the inputs into ZKFC. This is particularly
   * useful for running under jcarder to check for lock order violations.
   */
  @Test(timeout=30000)
  public void testOneOfEverything() throws Exception {
    try {
      cluster.start();
      
      // Failover by session expiration
      LOG.info("====== Failing over by session expiration");
      cluster.expireAndVerifyFailover(0, 1);
      cluster.expireAndVerifyFailover(1, 0);
      
      // Restart ZK
      LOG.info("====== Restarting server");
      stopServer();
      waitForServerDown(hostPort, CONNECTION_TIMEOUT);
      startServer();
      waitForServerUp(hostPort, CONNECTION_TIMEOUT);

      // Failover by bad health
      cluster.setHealthy(0, false);
      cluster.waitForHAState(0, HAServiceState.STANDBY);
      cluster.waitForHAState(1, HAServiceState.ACTIVE);
      cluster.setHealthy(1, true);
      cluster.setHealthy(0, false);
      cluster.waitForHAState(1, HAServiceState.ACTIVE);
      cluster.waitForHAState(0, HAServiceState.STANDBY);
      cluster.setHealthy(0, true);
      
      cluster.waitForHealthState(0, State.SERVICE_HEALTHY);
      
      // Graceful failovers
      cluster.getZkfc(1).gracefulFailoverToYou();
      cluster.getZkfc(0).gracefulFailoverToYou();
    } finally {
      cluster.stop();
    }
  }

  private int runFC(DummyHAService target, String ... args) throws Exception {
    DummyZKFC zkfc = new DummyZKFC(conf, target);
    return zkfc.run(args);
  }

}
