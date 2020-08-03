package top.folen.fastdfs;

import top.folen.common.FastDfsException;
import top.folen.common.IniFileReader;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Global variables
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class ClientGlobal {

	// 配置文件对应的key值

	/**
	 * 链接超时时间
	 */
	private static final String CONF_KEY_CONNECT_TIMEOUT = "connect_timeout";

	private static final String CONF_KEY_NETWORK_TIMEOUT = "network_timeout";
	private static final String CONF_KEY_CHARSET = "charset";
	private static final String CONF_KEY_HTTP_ANTI_STEAL_TOKEN = "http.anti_steal_token";
	private static final String CONF_KEY_HTTP_SECRET_KEY = "http.secret_key";
	private static final String CONF_KEY_HTTP_TRACKER_HTTP_PORT = "http.tracker_http_port";
	private static final String CONF_KEY_TRACKER_SERVER = "tracker_server";
	private static final String PROP_KEY_CONNECT_TIMEOUT_IN_SECONDS = "fastdfs.connect_timeout_in_seconds";
	private static final String PROP_KEY_NETWORK_TIMEOUT_IN_SECONDS = "fastdfs.network_timeout_in_seconds";
	private static final String PROP_KEY_CHARSET = "fastdfs.charset";
	private static final String PROP_KEY_HTTP_ANTI_STEAL_TOKEN = "fastdfs.http_anti_steal_token";
	private static final String PROP_KEY_HTTP_SECRET_KEY = "fastdfs.http_secret_key";
	private static final String PROP_KEY_HTTP_TRACKER_HTTP_PORT = "fastdfs.http_tracker_http_port";
	private static final String PROP_KEY_TRACKER_SERVERS = "fastdfs.tracker_servers";
	private static final String PROP_KEY_CONNECTION_POOL_ENABLED = "fastdfs.connection_pool.enabled";
	private static final String PROP_KEY_CONNECTION_POOL_MAX_COUNT_PER_ENTRY = "fastdfs.connection_pool" +
			".max_count_per_entry";
	private static final String PROP_KEY_CONNECTION_POOL_MAX_IDLE_TIME = "fastdfs.connection_pool.max_idle_time";
	private static final String PROP_KEY_CONNECTION_POOL_MAX_WAIT_TIME_IN_MS = "fastdfs.connection_pool" +
			".max_wait_time_in_ms";

	// 配置的具体属性值
	// second
	public static final int DEFAULT_CONNECT_TIMEOUT = 5;
	// second
	public static final int DEFAULT_NETWORK_TIMEOUT = 30;
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final boolean DEFAULT_HTTP_ANTI_STEAL_TOKEN = false;
	public static final String DEFAULT_HTTP_SECRET_KEY = "FastDFS1234567890";
	public static final int DEFAULT_HTTP_TRACKER_HTTP_PORT = 80;

	public static final boolean DEFAULT_CONNECTION_POOL_ENABLED = true;
	public static final int DEFAULT_CONNECTION_POOL_MAX_COUNT_PER_ENTRY = 100;
	//second
	public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE_TIME = 3600;
	//millisecond
	public static final int DEFAULT_CONNECTION_POOL_MAX_WAIT_TIME_IN_MS = 1000;
	//millisecond
	public static int g_connect_timeout = DEFAULT_CONNECT_TIMEOUT * 1000;
	//millisecond
	public static int g_network_timeout = DEFAULT_NETWORK_TIMEOUT * 1000;
	public static String G_CHARSET = DEFAULT_CHARSET;
	//if anti-steal token
	public static boolean g_anti_steal_token = DEFAULT_HTTP_ANTI_STEAL_TOKEN;
	//generate token secret key
	public static String g_secret_key = DEFAULT_HTTP_SECRET_KEY;
	public static int g_tracker_http_port = DEFAULT_HTTP_TRACKER_HTTP_PORT;

	public static boolean g_connection_pool_enabled = DEFAULT_CONNECTION_POOL_ENABLED;
	public static int g_connection_pool_max_count_per_entry = DEFAULT_CONNECTION_POOL_MAX_COUNT_PER_ENTRY;
	//millisecond
	public static int g_connection_pool_max_idle_time = DEFAULT_CONNECTION_POOL_MAX_IDLE_TIME * 1000;
	public static int g_connection_pool_max_wait_time_in_ms = DEFAULT_CONNECTION_POOL_MAX_WAIT_TIME_IN_MS;

	//millisecond
	public static TrackerGroup G_TRACKER_GROUP;

	private ClientGlobal() {
	}

	/**
	 * load global variables
	 *
	 * @param filename config filename
	 */
	public static void init(String filename) throws FastDfsException {

		IniFileReader iniReader = new IniFileReader(filename);
		// millisecond
		g_connect_timeout = iniReader.getPositiveIntValue(CONF_KEY_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT) * 1000;
		// millisecond
		g_network_timeout = iniReader.getPositiveIntValue(CONF_KEY_NETWORK_TIMEOUT, DEFAULT_NETWORK_TIMEOUT) * 1000;

		G_CHARSET = iniReader.getStrValue(CONF_KEY_CHARSET);
		if (G_CHARSET == null || G_CHARSET.length() == 0) {
			G_CHARSET = "ISO8859-1";
		}

		String[] szTrackerServers = iniReader.getValues(CONF_KEY_TRACKER_SERVER);
		if (szTrackerServers == null) {
			throw new FastDfsException("item \"tracker_server\" in " + filename + " not found");
		}

		InetSocketAddress[] trackerServers = new InetSocketAddress[szTrackerServers.length];
		String[] parts;
		for (int i = 0; i < szTrackerServers.length; i++) {
			parts = szTrackerServers[i].split(":", 2);
			if (parts.length != 2) {
				throw new FastDfsException("the value of item \"tracker_server\" is invalid, the correct format is " +
						"host:port");
			}

			trackerServers[i] = new InetSocketAddress(parts[0].trim(), Integer.parseInt(parts[1].trim()));
		}
		G_TRACKER_GROUP = new TrackerGroup(trackerServers);

		g_tracker_http_port = iniReader.getPositiveIntValue(CONF_KEY_HTTP_TRACKER_HTTP_PORT, 80);
		g_anti_steal_token = iniReader.getBoolValue(CONF_KEY_HTTP_ANTI_STEAL_TOKEN, false);
		if (g_anti_steal_token) {
			g_secret_key = iniReader.getStrValue(CONF_KEY_HTTP_SECRET_KEY);
		}
		g_connection_pool_enabled = iniReader.getBoolValue("connection_pool.enabled", DEFAULT_CONNECTION_POOL_ENABLED);
		g_connection_pool_max_count_per_entry = iniReader.getIntValue("connection_pool.max_count_per_entry",
				DEFAULT_CONNECTION_POOL_MAX_COUNT_PER_ENTRY);
		g_connection_pool_max_idle_time = iniReader.getPositiveIntValue("connection_pool.max_idle_time",
				DEFAULT_CONNECTION_POOL_MAX_IDLE_TIME) * 1000;

		g_connection_pool_max_wait_time_in_ms = iniReader.getPositiveIntValue("connection_pool.max_wait_time_in_ms",
				DEFAULT_CONNECTION_POOL_MAX_WAIT_TIME_IN_MS);
	}

	/**
	 * load from properties file
	 *
	 * @param propsFilePath properties file path, eg:
	 *                      "fastdfs-client.properties"
	 *                      "config/fastdfs-client.properties"
	 *                      "/opt/fastdfs-client.properties"
	 *                      "C:\\Users\\James\\config\\fastdfs-client.properties"
	 *                      properties文件至少包含一个配置项 fastdfs.tracker_servers 例如：
	 *                      fastdfs.tracker_servers = 10.0.11.245:22122,10.0.11.246:22122
	 *                      server的IP和端口用冒号':'分隔
	 *                      server之间用逗号','分隔
	 */
	public static void initByProperties(String propsFilePath) throws IOException, FastDfsException {
		Properties props = new Properties();
		InputStream in = IniFileReader.loadFromOsFileSystemOrClasspathAsStream(propsFilePath);
		if (in != null) {
			props.load(in);
		}
		initByProperties(props);
	}

	public static void initByProperties(Properties props) throws IOException, FastDfsException {
		String trackerServersConf = props.getProperty(PROP_KEY_TRACKER_SERVERS);
		if (trackerServersConf == null || trackerServersConf.trim().length() == 0) {
			throw new FastDfsException(String.format("configure item %s is required", PROP_KEY_TRACKER_SERVERS));
		}
		initByTrackers(trackerServersConf.trim());

		String connectTimeoutInSecondsConf = props.getProperty(PROP_KEY_CONNECT_TIMEOUT_IN_SECONDS);
		String networkTimeoutInSecondsConf = props.getProperty(PROP_KEY_NETWORK_TIMEOUT_IN_SECONDS);
		String charsetConf = props.getProperty(PROP_KEY_CHARSET);
		String httpAntiStealTokenConf = props.getProperty(PROP_KEY_HTTP_ANTI_STEAL_TOKEN);
		String httpSecretKeyConf = props.getProperty(PROP_KEY_HTTP_SECRET_KEY);
		String httpTrackerHttpPortConf = props.getProperty(PROP_KEY_HTTP_TRACKER_HTTP_PORT);
		String poolEnabled = props.getProperty(PROP_KEY_CONNECTION_POOL_ENABLED);
		String poolMaxCountPerEntry = props.getProperty(PROP_KEY_CONNECTION_POOL_MAX_COUNT_PER_ENTRY);
		String poolMaxIdleTime = props.getProperty(PROP_KEY_CONNECTION_POOL_MAX_IDLE_TIME);
		String poolMaxWaitTimeInMs = props.getProperty(PROP_KEY_CONNECTION_POOL_MAX_WAIT_TIME_IN_MS);

		if (connectTimeoutInSecondsConf != null && connectTimeoutInSecondsConf.trim().length() != 0) {
			g_connect_timeout = Integer.parseInt(connectTimeoutInSecondsConf.trim()) * 1000;
		}
		if (networkTimeoutInSecondsConf != null && networkTimeoutInSecondsConf.trim().length() != 0) {
			g_network_timeout = Integer.parseInt(networkTimeoutInSecondsConf.trim()) * 1000;
		}
		if (charsetConf != null && charsetConf.trim().length() != 0) {
			G_CHARSET = charsetConf.trim();
		}
		if (httpAntiStealTokenConf != null && httpAntiStealTokenConf.trim().length() != 0) {
			g_anti_steal_token = Boolean.parseBoolean(httpAntiStealTokenConf);
		}
		if (httpSecretKeyConf != null && httpSecretKeyConf.trim().length() != 0) {
			g_secret_key = httpSecretKeyConf.trim();
		}
		if (httpTrackerHttpPortConf != null && httpTrackerHttpPortConf.trim().length() != 0) {
			g_tracker_http_port = Integer.parseInt(httpTrackerHttpPortConf);
		}
		if (poolEnabled != null && poolEnabled.trim().length() != 0) {
			g_connection_pool_enabled = Boolean.parseBoolean(poolEnabled);
		}
		if (poolMaxCountPerEntry != null && poolMaxCountPerEntry.trim().length() != 0) {
			g_connection_pool_max_count_per_entry = Integer.parseInt(poolMaxCountPerEntry);
		}
		if (poolMaxIdleTime != null && poolMaxIdleTime.trim().length() != 0) {
			g_connection_pool_max_idle_time = Integer.parseInt(poolMaxIdleTime) * 1000;
		}
		if (poolMaxWaitTimeInMs != null && poolMaxWaitTimeInMs.trim().length() != 0) {
			g_connection_pool_max_wait_time_in_ms = Integer.parseInt(poolMaxWaitTimeInMs);
		}
	}

	/**
	 * load from properties file
	 *
	 * @param trackerServers 例如："10.0.11.245:22122,10.0.11.246:22122"
	 *                       server的IP和端口用冒号':'分隔
	 *                       server之间用逗号','分隔
	 */
	public static void initByTrackers(String trackerServers) throws IOException, FastDfsException {
		List<InetSocketAddress> list = new ArrayList<>();
		String spr1 = ",";
		String spr2 = ":";
		String[] arr1 = trackerServers.trim().split(spr1);
		for (String addrStr : arr1) {
			String[] arr2 = addrStr.trim().split(spr2);
			String host = arr2[0].trim();
			int port = Integer.parseInt(arr2[1].trim());
			list.add(new InetSocketAddress(host, port));
		}
		InetSocketAddress[] trackerAddresses = list.toArray(new InetSocketAddress[list.size()]);
		initByTrackers(trackerAddresses);
	}

	public static void initByTrackers(InetSocketAddress[] trackerAddresses) throws IOException, FastDfsException {
		G_TRACKER_GROUP = new TrackerGroup(trackerAddresses);
	}

	/**
	 * construct Socket object
	 *
	 * @param ipAddr ip address or hostname
	 * @param port   port number
	 * @return connected Socket object
	 */
	public static Socket getSocket(String ipAddr, int port) throws IOException {
		Socket sock = new Socket();
		sock.setSoTimeout(ClientGlobal.g_network_timeout);
		sock.connect(new InetSocketAddress(ipAddr, port), ClientGlobal.g_connect_timeout);
		return sock;
	}

	/**
	 * construct Socket object
	 *
	 * @param addr InetSocketAddress object, including ip address and port
	 * @return connected Socket object
	 */
	public static Socket getSocket(InetSocketAddress addr) throws IOException {
		Socket sock = new Socket();
		sock.setReuseAddress(true);
		sock.setSoTimeout(ClientGlobal.g_network_timeout);
		sock.connect(addr, ClientGlobal.g_connect_timeout);
		return sock;
	}

	public static String configInfo() {
		StringBuilder trackerServers = new StringBuilder();
		if (G_TRACKER_GROUP != null) {
			InetSocketAddress[] trackerAddresses = G_TRACKER_GROUP.trackerServers;
			for (InetSocketAddress inetSocketAddress : trackerAddresses) {
				if (trackerServers.length() > 0) {
					trackerServers.append(",");
				}
				trackerServers.append(inetSocketAddress.toString().substring(1));
			}
		}
		return "{"
				+ "\n  g_connect_timeout(ms) = " + g_connect_timeout
				+ "\n  g_network_timeout(ms) = " + g_network_timeout
				+ "\n  G_CHARSET = " + G_CHARSET
				+ "\n  g_anti_steal_token = " + g_anti_steal_token
				+ "\n  g_secret_key = " + g_secret_key
				+ "\n  g_tracker_http_port = " + g_tracker_http_port
				+ "\n  g_connection_pool_enabled = " + g_connection_pool_enabled
				+ "\n  g_connection_pool_max_count_per_entry = " + g_connection_pool_max_count_per_entry
				+ "\n  g_connection_pool_max_idle_time(ms) = " + g_connection_pool_max_idle_time
				+ "\n  g_connection_pool_max_wait_time_in_ms(ms) = " + g_connection_pool_max_wait_time_in_ms
				+ "\n  trackerServers = " + trackerServers
				+ "\n}";
	}

}
