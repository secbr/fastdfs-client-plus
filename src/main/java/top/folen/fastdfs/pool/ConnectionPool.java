package top.folen.fastdfs.pool;

import top.folen.common.FastDfsException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 连接池相关操作
 *
 * @author zzs
 */
public class ConnectionPool {

	/**
	 * key is ip:port, value is ConnectionManager
	 */
	private final static ConcurrentHashMap<String, ConnectionManager> CP = new ConcurrentHashMap<>();

	public static Connection getConnection(InetSocketAddress socketAddress) throws FastDfsException {
		if (socketAddress == null) {
			return null;
		}
		String key = getKey(socketAddress);
		ConnectionManager connectionManager;
		connectionManager = CP.get(key);
		if (connectionManager == null) {
			synchronized (ConnectionPool.class) {
				connectionManager = CP.get(key);
				if (connectionManager == null) {
					connectionManager = new ConnectionManager(socketAddress);
					CP.put(key, connectionManager);
				}
			}
		}
		return connectionManager.getConnection();
	}

	public static void releaseConnection(Connection connection) throws IOException {
		if (connection == null) {
			return;
		}
		String key = getKey(connection.getInetSocketAddress());
		ConnectionManager connectionManager = CP.get(key);
		if (connectionManager != null) {
			connectionManager.releaseConnection(connection);
		} else {
			connection.closeDirectly();
		}

	}

	public static void closeConnection(Connection connection) throws IOException {
		if (connection == null) {
			return;
		}
		String key = getKey(connection.getInetSocketAddress());
		ConnectionManager connectionManager = CP.get(key);
		if (connectionManager != null) {
			connectionManager.closeConnection(connection);
			connectionManager.setActiveTestFlag();
		} else {
			connection.closeDirectly();
		}
	}

	private static String getKey(InetSocketAddress socketAddress) {
		if (socketAddress == null) {
			return null;
		}
		return String.format("%s:%s", socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
	}

	@Override
	public String toString() {
		if (!CP.isEmpty()) {
			StringBuilder builder = new StringBuilder();
			for (Map.Entry<String, ConnectionManager> managerEntry : CP.entrySet()) {
				builder.append("key:[").append(managerEntry.getKey()).append(" ]-------- entry:").append(managerEntry.getValue()).append("\n");
			}
			return builder.toString();
		}
		return null;
	}
}
