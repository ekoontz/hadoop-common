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

package org.apache.hadoop.hdfs.web;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.ConcatSourcesParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.RenameOptionSetParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/** A FileSystem for HDFS over the web. */
public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {
  public static final Log LOG = LogFactory.getLog(WebHdfsFileSystem.class);
  /** File System URI: {SCHEME}://namenode:port/path/to/file */
  public static final String SCHEME = "webhdfs";
  /** WebHdfs version. */
  public static final int VERSION = 1;
  /** Http URI: http://namenode:port/{PATH_PREFIX}/path/to/file */
  public static final String PATH_PREFIX = "/" + SCHEME + "/v" + VERSION;

  /** Default connection factory may be overridden in tests to use smaller timeout values */
  protected URLConnectionFactory connectionFactory;

  /** Delegation token kind */
  public static final Text TOKEN_KIND = new Text("WEBHDFS delegation");
  protected TokenAspect<? extends WebHdfsFileSystem> tokenAspect;

  private UserGroupInformation ugi;
  private URI uri;
  private Token<?> delegationToken;
  protected Text tokenServiceName;
  private RetryPolicy retryPolicy = null;
  private Path workingDir;
  private InetSocketAddress nnAddrs[];
  private int currentNNAddrIndex;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>webhdfs</code>
   */
  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * return the underlying transport protocol (http / https).
   */
  protected String getTransportScheme() {
    return "http";
  }

  /**
   * Initialize tokenAspect. This function is intended to
   * be overridden by SWebHdfsFileSystem.
   */
  protected synchronized void initializeTokenAspect() {
    tokenAspect = new TokenAspect<WebHdfsFileSystem>(this, tokenServiceName,
        TOKEN_KIND);
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf
      ) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    /** set user pattern based on configuration file */
    UserParam.setUserPattern(conf.get(
        DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
        DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));

    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);

    ugi = UserGroupInformation.getCurrentUser();
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.nnAddrs = DFSUtil.resolveWebHdfsUri(this.uri, conf);

    boolean isHA = HAUtil.isLogicalUri(conf, this.uri);
    // In non-HA case, the code needs to call getCanonicalUri() in order to
    // handle the case where no port is specified in the URI
    this.tokenServiceName = isHA ? HAUtil.buildTokenServiceForLogicalUri(uri)
        : SecurityUtil.buildTokenService(getCanonicalUri());
    initializeTokenAspect();

    if (!isHA) {
      this.retryPolicy =
          RetryUtils.getDefaultRetryPolicy(
              conf,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
              SafeModeException.class);
    } else {

      int maxFailoverAttempts = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
      int failoverSleepBaseMillis = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      int failoverSleepMaxMillis = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);

      this.retryPolicy = RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              maxFailoverAttempts, maxRetryAttempts, failoverSleepBaseMillis,
              failoverSleepMaxMillis);
    }

    this.workingDir = getHomeDirectory();

    if (UserGroupInformation.isSecurityEnabled()) {
      tokenAspect.initDelegationToken(ugi);
    }
  }

  @Override
  public URI getCanonicalUri() {
    return super.getCanonicalUri();
  }

  /** Is WebHDFS enabled in conf? */
  public static boolean isEnabled(final Configuration conf, final Log log) {
    final boolean b = conf.getBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        DFSConfigKeys.DFS_WEBHDFS_ENABLED_DEFAULT);
    return b;
  }

  protected synchronized Token<?> getDelegationToken() throws IOException {
    tokenAspect.ensureTokenInitialized();
    return delegationToken;
  }

  @Override
  protected int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  @Override
  public URI getUri() {
    return this.uri;
  }
  
  @Override
  protected URI canonicalizeUri(URI uri) {
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
  }

  /** @return the home directory. */
  public static String getHomeDirectoryString(final UserGroupInformation ugi) {
    return "/user/" + ugi.getShortUserName();
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(getHomeDirectoryString(ugi)));
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public synchronized void setWorkingDirectory(final Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path f) {
    return f.isAbsolute()? f: new Path(workingDir, f);
  }

  static Map<?, ?> jsonParse(final HttpURLConnection c, final boolean useErrorStream
      ) throws IOException {
    if (c.getContentLength() == 0) {
      return null;
    }
    final InputStream in = useErrorStream? c.getErrorStream(): c.getInputStream();
    if (in == null) {
      throw new IOException("The " + (useErrorStream? "error": "input") + " stream is null.");
    }
    final String contentType = c.getContentType();
    if (contentType != null) {
      final MediaType parsed = MediaType.valueOf(contentType);
      if (!MediaType.APPLICATION_JSON_TYPE.isCompatible(parsed)) {
        throw new IOException("Content-Type \"" + contentType
            + "\" is incompatible with \"" + MediaType.APPLICATION_JSON
            + "\" (parsed=\"" + parsed + "\")");
      }
    }
    return (Map<?, ?>)JSON.parse(new InputStreamReader(in, Charsets.UTF_8));
  }

  private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
      final HttpURLConnection conn, boolean unwrapException) throws IOException {
    final int code = conn.getResponseCode();
    // server is demanding an authentication we don't support
    if (code == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new IOException(
          new AuthenticationException(conn.getResponseMessage()));
    }
    if (code != op.getExpectedHttpResponseCode()) {
      final Map<?, ?> m;
      try {
        m = jsonParse(conn, true);
      } catch(Exception e) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage(), e);
      }

      if (m == null) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage());
      } else if (m.get(RemoteException.class.getSimpleName()) == null) {
        return m;
      }

      final RemoteException re = JsonUtil.toRemoteException(m);
      throw unwrapException? toIOException(re): re;
    }
    return null;
  }

  /**
   * Covert an exception to an IOException.
   * 
   * For a non-IOException, wrap it with IOException.
   * For a RemoteException, unwrap it.
   * For an IOException which is not a RemoteException, return it. 
   */
  private static IOException toIOException(Exception e) {
    if (!(e instanceof IOException)) {
      return new IOException(e);
    }

    final IOException ioe = (IOException)e;
    if (!(ioe instanceof RemoteException)) {
      return ioe;
    }

    return ((RemoteException)ioe).unwrapRemoteException();
  }

  private synchronized InetSocketAddress getCurrentNNAddr() {
    return nnAddrs[currentNNAddrIndex];
  }

  /**
   * Reset the appropriate state to gracefully fail over to another name node
   */
  private synchronized void resetStateToFailOver() {
    currentNNAddrIndex = (currentNNAddrIndex + 1) % nnAddrs.length;
    delegationToken = null;
    tokenAspect.reset();
  }

  /**
   * Return a URL pointing to given path on the namenode.
   *
   * @param path to obtain the URL for
   * @param query string to append to the path
   * @return namenode URL referring to the given path
   * @throws IOException on error constructing the URL
   */
  private URL getNamenodeURL(String path, String query) throws IOException {
    InetSocketAddress nnAddr = getCurrentNNAddr();
    final URL url = new URL(getTransportScheme(), nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }
  
  Param<?,?>[] getAuthParameters(final HttpOpParam.Op op) throws IOException {
    List<Param<?,?>> authParams = Lists.newArrayList();    
    // Skip adding delegation token for token operations because these
    // operations require authentication.
    Token<?> token = null;
    if (UserGroupInformation.isSecurityEnabled() && !op.getRequireAuth()) {
      token = getDelegationToken();
    }
    if (token != null) {
      authParams.add(new DelegationParam(token.encodeToUrlString()));
    } else {
      UserGroupInformation userUgi = ugi;
      UserGroupInformation realUgi = userUgi.getRealUser();
      if (realUgi != null) { // proxy user
        authParams.add(new DoAsParam(userUgi.getShortUserName()));
        userUgi = realUgi;
      }
      authParams.add(new UserParam(userUgi.getShortUserName()));
    }
    return authParams.toArray(new Param<?,?>[0]);
  }

  URL toUrl(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    //initialize URI path and query
    final String path = PATH_PREFIX
        + (fspath == null? "/": makeQualified(fspath).toUri().getRawPath());
    final String query = op.toQueryString()
        + Param.toSortedString("&", getAuthParameters(op))
        + Param.toSortedString("&", parameters);
    final URL url = getNamenodeURL(path, query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  /**
   * Run a http operation.
   * Connect to the http server, validate response, and obtain the JSON output.
   * 
   * @param op http operation
   * @param fspath file system path
   * @param parameters parameters for the operation
   * @return a JSON object, e.g. Object[], Map<?, ?>, etc.
   * @throws IOException
   */
  private Map<?, ?> run(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    return new FsPathRunner(op, fspath, parameters).run().json;
  }

  /**
   * This class is for initialing a HTTP connection, connecting to server,
   * obtaining a response, and also handling retry on failures.
   */
  abstract class AbstractRunner {
    abstract protected URL getUrl() throws IOException;

    protected final HttpOpParam.Op op;
    private final boolean redirected;

    private boolean checkRetry;
    protected HttpURLConnection conn = null;
    private Map<?, ?> json = null;

    protected AbstractRunner(final HttpOpParam.Op op, boolean redirected) {
      this.op = op;
      this.redirected = redirected;
    }

    AbstractRunner run() throws IOException {
      UserGroupInformation connectUgi = ugi.getRealUser();
      if (connectUgi == null) {
        connectUgi = ugi;
      }
      if (op.getRequireAuth()) {
        connectUgi.checkTGTAndReloginFromKeytab();
      }
      try {
        // the entire lifecycle of the connection must be run inside the
        // doAs to ensure authentication is performed correctly
        return connectUgi.doAs(
            new PrivilegedExceptionAction<AbstractRunner>() {
              @Override
              public AbstractRunner run() throws IOException {
                return runWithRetry();
              }
            });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    
    private void init() throws IOException {
      checkRetry = !redirected;
      URL url = getUrl();
      conn = (HttpURLConnection) connectionFactory.openConnection(url);
    }
    
    private void connect() throws IOException {
      connect(op.getDoOutput());
    }

    private void connect(boolean doOutput) throws IOException {
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(doOutput);
      conn.setInstanceFollowRedirects(false);
      conn.connect();
    }

    private void disconnect() {
      if (conn != null) {
        conn.disconnect();
        conn = null;
      }
    }

    private AbstractRunner runWithRetry() throws IOException {
      /**
       * Do the real work.
       *
       * There are three cases that the code inside the loop can throw an
       * IOException:
       *
       * <ul>
       * <li>The connection has failed (e.g., ConnectException,
       * @see FailoverOnNetworkExceptionRetry for more details)</li>
       * <li>The namenode enters the standby state (i.e., StandbyException).</li>
       * <li>The server returns errors for the command (i.e., RemoteException)</li>
       * </ul>
       *
       * The call to shouldRetry() will conduct the retry policy. The policy
       * examines the exception and swallows it if it decides to rerun the work.
       */
      for(int retry = 0; ; retry++) {
        try {
          init();
          if (op.getDoOutput()) {
            twoStepWrite();
          } else {
            getResponse(op != GetOpParam.Op.OPEN);
          }
          return this;
        } catch(IOException ioe) {
          Throwable cause = ioe.getCause();
          if (cause != null && cause instanceof AuthenticationException) {
            throw ioe; // no retries for auth failures
          }
          shouldRetry(ioe, retry);
        }
      }
    }

    private void shouldRetry(final IOException ioe, final int retry
        ) throws IOException {
      InetSocketAddress nnAddr = getCurrentNNAddr();
      if (checkRetry) {
        try {
          final RetryPolicy.RetryAction a = retryPolicy.shouldRetry(
              ioe, retry, 0, true);

          boolean isRetry = a.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
          boolean isFailoverAndRetry =
              a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

          if (isRetry || isFailoverAndRetry) {
            LOG.info("Retrying connect to namenode: " + nnAddr
                + ". Already tried " + retry + " time(s); retry policy is "
                + retryPolicy + ", delay " + a.delayMillis + "ms.");

            if (isFailoverAndRetry) {
              resetStateToFailOver();
            }

            Thread.sleep(a.delayMillis);
            return;
          }
        } catch(Exception e) {
          LOG.warn("Original exception is ", ioe);
          throw toIOException(e);
        }
      }
      throw toIOException(ioe);
    }

    /**
     * Two-step Create/Append:
     * Step 1) Submit a Http request with neither auto-redirect nor data. 
     * Step 2) Submit another Http request with the URL from the Location header with data.
     * 
     * The reason of having two-step create/append is for preventing clients to
     * send out the data before the redirect. This issue is addressed by the
     * "Expect: 100-continue" header in HTTP/1.1; see RFC 2616, Section 8.2.3.
     * Unfortunately, there are software library bugs (e.g. Jetty 6 http server
     * and Java 6 http client), which do not correctly implement "Expect:
     * 100-continue". The two-step create/append is a temporary workaround for
     * the software library bugs.
     */
    HttpURLConnection twoStepWrite() throws IOException {
      //Step 1) Submit a Http request with neither auto-redirect nor data. 
      connect(false);
      validateResponse(HttpOpParam.TemporaryRedirectOp.valueOf(op), conn, false);
      final String redirect = conn.getHeaderField("Location");
      disconnect();
      checkRetry = false;
      
      //Step 2) Submit another Http request with the URL from the Location header with data.
      conn = (HttpURLConnection) connectionFactory.openConnection(new URL(
          redirect));
      conn.setRequestProperty("Content-Type",
          MediaType.APPLICATION_OCTET_STREAM);
      conn.setChunkedStreamingMode(32 << 10); //32kB-chunk
      connect();
      return conn;
    }

    FSDataOutputStream write(final int bufferSize) throws IOException {
      return WebHdfsFileSystem.this.write(op, conn, bufferSize);
    }

    void getResponse(boolean getJsonAndDisconnect) throws IOException {
      try {
        connect();
        final int code = conn.getResponseCode();
        if (!redirected && op.getRedirect()
            && code != op.getExpectedHttpResponseCode()) {
          final String redirect = conn.getHeaderField("Location");
          json = validateResponse(HttpOpParam.TemporaryRedirectOp.valueOf(op),
              conn, false);
          disconnect();
  
          checkRetry = false;
          conn = (HttpURLConnection) connectionFactory.openConnection(new URL(
              redirect));
          connect();
        }

        json = validateResponse(op, conn, false);
        if (json == null && getJsonAndDisconnect) {
          json = jsonParse(conn, false);
        }
      } finally {
        if (getJsonAndDisconnect) {
          disconnect();
        }
      }
    }
  }

  final class FsPathRunner extends AbstractRunner {
    private final Path fspath;
    private final Param<?, ?>[] parameters;

    FsPathRunner(final HttpOpParam.Op op, final Path fspath, final Param<?,?>... parameters) {
      super(op, false);
      this.fspath = fspath;
      this.parameters = parameters;
    }

    @Override
    protected URL getUrl() throws IOException {
      return toUrl(op, fspath, parameters);
    }
  }

  final class URLRunner extends AbstractRunner {
    private final URL url;
    @Override
    protected URL getUrl() {
      return url;
    }

    protected URLRunner(final HttpOpParam.Op op, final URL url, boolean redirected) {
      super(op, redirected);
      this.url = url;
    }
  }

  private FsPermission applyUMask(FsPermission permission) {
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    return permission.applyUMask(FsPermission.getUMask(getConf()));
  }

  private HdfsFileStatus getHdfsFileStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETFILESTATUS;
    final Map<?, ?> json = run(op, f);
    final HdfsFileStatus status = JsonUtil.toFileStatus(json, true);
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return makeQualified(getHdfsFileStatus(f), f);
  }

  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink() ? new Path(f.getSymlink()) : null,
        f.getFullPath(parent).makeQualified(getUri(), getWorkingDirectory()));
  }

  @Override
  public AclStatus getAclStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETACLSTATUS;
    final Map<?, ?> json = run(op, f);
    AclStatus status = JsonUtil.toAclStatus(json);
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MKDIRS;
    final Map<?, ?> json = run(op, f,
        new PermissionParam(applyUMask(permission)));
    return (Boolean)json.get("boolean");
  }

  /**
   * Create a symlink pointing to the destination path.
   * @see org.apache.hadoop.fs.Hdfs#createSymlink(Path, Path, boolean) 
   */
  public void createSymlink(Path destination, Path f, boolean createParent
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESYMLINK;
    run(op, f, new DestinationParam(makeQualified(destination).toUri().getPath()),
        new CreateParentParam(createParent));
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    final Map<?, ?> json = run(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath()));
    return (Boolean)json.get("boolean");
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    run(op, src, new DestinationParam(makeQualified(dst).toUri().getPath()),
        new RenameOptionSetParam(options));
  }

  @Override
  public void setOwner(final Path p, final String owner, final String group
      ) throws IOException {
    if (owner == null && group == null) {
      throw new IOException("owner == null && group == null");
    }

    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETOWNER;
    run(op, p, new OwnerParam(owner), new GroupParam(group));
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
    run(op, p, new PermissionParam(permission));
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MODIFYACLENTRIES;
    run(op, path, new AclPermissionParam(aclSpec));
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACLENTRIES;
    run(op, path, new AclPermissionParam(aclSpec));
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEDEFAULTACL;
    run(op, path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACL;
    run(op, path);
  }

  @Override
  public void setAcl(final Path p, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETACL;
    run(op, p, new AclPermissionParam(aclSpec));
  }

  @Override
  public boolean setReplication(final Path p, final short replication
     ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
    final Map<?, ?> json = run(op, p, new ReplicationParam(replication));
    return (Boolean)json.get("boolean");
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETTIMES;
    run(op, p, new ModificationTimeParam(mtime), new AccessTimeParam(atime));
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short)getConf().getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  FSDataOutputStream write(final HttpOpParam.Op op,
      final HttpURLConnection conn, final int bufferSize) throws IOException {
    return new FSDataOutputStream(new BufferedOutputStream(
        conn.getOutputStream(), bufferSize), statistics) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          try {
            validateResponse(op, conn, true);
          } finally {
            conn.disconnect();
          }
        }
      }
    };
  }

  @Override
  public void concat(final Path trg, final Path [] srcs) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PostOpParam.Op.CONCAT;

    ConcatSourcesParam param = new ConcatSourcesParam(srcs);
    run(op, trg, param);
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    return new FsPathRunner(op, f,
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize))
      .run()
      .write(bufferSize);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.APPEND;
    return new FsPathRunner(op, f, new BufferSizeParam(bufferSize))
      .run()
      .write(bufferSize);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETE;
    final Map<?, ?> json = run(op, f, new RecursiveParam(recursive));
    return (Boolean)json.get("boolean");
  }

  @Override
  public FSDataInputStream open(final Path f, final int buffersize
      ) throws IOException {
    statistics.incrementReadOps(1);
    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    final URL url = toUrl(op, f, new BufferSizeParam(buffersize));
    return new FSDataInputStream(new OffsetUrlInputStream(
        new OffsetUrlOpener(url), new OffsetUrlOpener(null)));
  }

  @Override
  public void close() throws IOException {
    super.close();
    synchronized (this) {
      tokenAspect.removeRenewAction();
    }
  }

  class OffsetUrlOpener extends ByteRangeInputStream.URLOpener {
    OffsetUrlOpener(final URL url) {
      super(url);
    }

    /** Setup offset url and connect. */
    @Override
    protected HttpURLConnection connect(final long offset,
        final boolean resolved) throws IOException {
      final URL offsetUrl = offset == 0L? url
          : new URL(url + "&" + new OffsetParam(offset));
      return new URLRunner(GetOpParam.Op.OPEN, offsetUrl, resolved).run().conn;
    }  
  }

  private static final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";

  /** Remove offset parameter, if there is any, from the url */
  static URL removeOffsetParam(final URL url) throws MalformedURLException {
    String query = url.getQuery();
    if (query == null) {
      return url;
    }
    final String lower = query.toLowerCase();
    if (!lower.startsWith(OFFSET_PARAM_PREFIX)
        && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
      return url;
    }

    //rebuild query
    StringBuilder b = null;
    for(final StringTokenizer st = new StringTokenizer(query, "&");
        st.hasMoreTokens();) {
      final String token = st.nextToken();
      if (!token.toLowerCase().startsWith(OFFSET_PARAM_PREFIX)) {
        if (b == null) {
          b = new StringBuilder("?").append(token);
        } else {
          b.append('&').append(token);
        }
      }
    }
    query = b == null? "": b.toString();

    final String urlStr = url.toString();
    return new URL(urlStr.substring(0, urlStr.indexOf('?')) + query);
  }

  static class OffsetUrlInputStream extends ByteRangeInputStream {
    OffsetUrlInputStream(OffsetUrlOpener o, OffsetUrlOpener r) {
      super(o, r);
    }

    /** Remove offset parameter before returning the resolved url. */
    @Override
    protected URL getResolvedUrl(final HttpURLConnection connection
        ) throws MalformedURLException {
      return removeOffsetParam(connection.getURL());
    }
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.LISTSTATUS;
    final Map<?, ?> json  = run(op, f);
    final Map<?, ?> rootmap = (Map<?, ?>)json.get(FileStatus.class.getSimpleName() + "es");
    final Object[] array = (Object[])rootmap.get(FileStatus.class.getSimpleName());

    //convert FileStatus
    final FileStatus[] statuses = new FileStatus[array.length];
    for(int i = 0; i < array.length; i++) {
      final Map<?, ?> m = (Map<?, ?>)array[i];
      statuses[i] = makeQualified(JsonUtil.toFileStatus(m, false), f);
    }
    return statuses;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETDELEGATIONTOKEN;
    final Map<?, ?> m = run(op, null, new RenewerParam(renewer));
    final Token<DelegationTokenIdentifier> token = JsonUtil.toDelegationToken(m);
    token.setService(tokenServiceName);
    return token;
  }

  @Override
  public synchronized Token<?> getRenewToken() {
    return delegationToken;
  }

  @Override
  public <T extends TokenIdentifier> void setDelegationToken(
      final Token<T> token) {
    synchronized (this) {
      delegationToken = token;
    }
  }

  @Override
  public synchronized long renewDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.RENEWDELEGATIONTOKEN;
    TokenArgumentParam dtargParam = new TokenArgumentParam(
        token.encodeToUrlString());
    final Map<?, ?> m = run(op, null, dtargParam);
    return (Long) m.get("long");
  }

  @Override
  public synchronized void cancelDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.CANCELDELEGATIONTOKEN;
    TokenArgumentParam dtargParam = new TokenArgumentParam(
        token.encodeToUrlString());
    run(op, null, dtargParam);
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }
    return getFileBlockLocations(status.getPath(), offset, length);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path p, 
      final long offset, final long length) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GET_BLOCK_LOCATIONS;
    final Map<?, ?> m = run(op, p, new OffsetParam(offset),
        new LengthParam(length));
    return DFSUtil.locatedBlocks2Locations(JsonUtil.toLocatedBlocks(m));
  }

  @Override
  public ContentSummary getContentSummary(final Path p) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GETCONTENTSUMMARY;
    final Map<?, ?> m = run(op, p);
    return JsonUtil.toContentSummary(m);
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(final Path p
      ) throws IOException {
    statistics.incrementReadOps(1);
  
    final HttpOpParam.Op op = GetOpParam.Op.GETFILECHECKSUM;
    final Map<?, ?> m = run(op, p);
    return JsonUtil.toMD5MD5CRC32FileChecksum(m);
  }

  @Override
  public String getCanonicalServiceName() {
    return tokenServiceName == null ? super.getCanonicalServiceName()
        : tokenServiceName.toString();
  }

  @VisibleForTesting
  InetSocketAddress[] getResolvedNNAddr() {
    return nnAddrs;
  }
}
