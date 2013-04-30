package org.apache.hadoop.mapred;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.PiEstimator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite to verify whether userlogs are retained across daemon restarts
 */
public class TestUserlogsRetentionAcrossRestarts {
  private Configuration conf = new Configuration();
  private MiniDFSCluster dfs = null;
  private MiniMRCluster mr = null;

  private static final int numDir = 1;
  private static final int numDataNodes = 1;
  private static final int numTaskTrackers = 1;

  {
    /**
     * As these tests involve restarting JT/TT, they take really long and should
     * not be part of CI runs. However, one should be able to run the test if
     * required by passing it as an argument to -Dtestcase
     */
    String testName = System.getProperty("testcase");
    Assume.assumeTrue(testName != null
        && testName.contains(TestUserlogsRetentionAcrossRestarts.class
            .getSimpleName()));
  }

  @Before
  public void startCluster() throws IOException {
    dfs = new MiniDFSCluster(conf, numDataNodes, true, null);
    mr = new MiniMRCluster(numTaskTrackers, dfs.getFileSystem().getUri()
        .toString(), numDir);
    mr.waitUntilIdle();
  }

  @After
  public void stopCluster() {
    if (dfs != null) {
      dfs.shutdown();
    }
    if (mr != null) {
      mr.shutdown();
    }
  }
  
  private void runPiEstimator() throws IOException {
    JobConf jobConf = mr.createJobConf();
    PiEstimator.estimate(2, 100, jobConf);
  }

  private void restartTaskTracker(int id) throws IOException {
    mr.stopTaskTracker(id);
    mr.startTaskTracker(null, null, id, numDir);
    mr.waitUntilIdle();
  }

  /**
   * Test verifies if userlogs are retained on TT restart.
   * 
   * @throws IOException
   */
  @Test
  public void testOnTaskTrackerRestart() throws IOException {
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    String ttDir = mr.getTaskTrackerLocalDirs(0)[0];
    Path userlogsPath = new Path(ttDir, TaskLog.USERLOGS_DIR_NAME);

    runPiEstimator();
    assertTrue("Userlogs should exist before TT shutdown",
        localFs.exists(userlogsPath));

    restartTaskTracker(0);

    assertTrue("Userlogs should not be deleted on restart",
        localFs.exists(userlogsPath));
  }

  /**
   * Test verifies if userlogs are retained on JT restart. In cluster setups
   * where JT is co-located with one of the TTs, JT restart should not delete TT
   * userlogs.
   * 
   * @throws IOException
   */
  @Test
  public void testOnJobTrackerRestart() throws IOException {
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    String ttDir = mr.getTaskTrackerLocalDirs(0)[0];
    Path userlogsPath = new Path(ttDir, TaskLog.USERLOGS_DIR_NAME);

    runPiEstimator();
    assertTrue("Userlogs should exist before JT shutdown",
        localFs.exists(userlogsPath));

    mr.stopJobTracker();
    mr.startJobTracker(true);

    assertTrue("Userlogs should not be deleted on restart",
        localFs.exists(userlogsPath));
  }
}
