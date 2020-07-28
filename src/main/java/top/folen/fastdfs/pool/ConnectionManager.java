package top.folen.fastdfs.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.folen.common.FastDfsException;
import top.folen.fastdfs.ClientGlobal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionManager {

	private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

	private InetSocketAddress inetSocketAddress;

	/**
	 * total create connection pool
	 */
	private final AtomicInteger totalCount = new AtomicInteger();

	/**
	 * free connection count
	 */
	private final AtomicInteger freeCount = new AtomicInteger();

	/**
	 * lock
	 */
	private final ReentrantLock lock = new ReentrantLock(true);

	private final Condition condition = lock.newCondition();

	/**
	 * free connections
	 */
	private final LinkedList<Connection> freeConnections = new LinkedList<>();

	private ConnectionManager() {
	}

	public ConnectionManager(InetSocketAddress socketAddress) {
		this.inetSocketAddress = socketAddress;
	}

	public Connection getConnection() throws FastDfsException {
		lock.lock();
		try {
			Connection connection;
			while (true) {
				if (freeCount.get() > 0) {
					freeCount.decrementAndGet();
					connection = freeConnections.poll();
					if (!connection.isAvailable() || (System.currentTimeMillis() - connection.getLastAccessTime()) > ClientGlobal.g_connection_pool_max_idle_time) {
						closeConnection(connection);
						continue;
					}
					if (connection.isNeedActiveTest()) {
						boolean isActive;
						try {
							isActive = connection.activeTest();
						} catch (IOException e) {
							System.err.println("send to server[" + inetSocketAddress.getAddress().getHostAddress() +
									":" + inetSocketAddress.getPort() + "] active test error ,emsg:" + e.getMessage());
							isActive = false;
						}
						if (!isActive) {
							closeConnection(connection);
							continue;
						} else {
							connection.setNeedActiveTest(false);
						}
					}
				} else if (ClientGlobal.g_connection_pool_max_count_per_entry == 0 || totalCount.get() < ClientGlobal.g_connection_pool_max_count_per_entry) {
					connection = ConnectionUtil.create(this.inetSocketAddress);
					totalCount.incrementAndGet();
				} else {
					try {
						if (condition.await(ClientGlobal.g_connection_pool_max_wait_time_in_ms,
								TimeUnit.MILLISECONDS)) {
							//wait single success
							continue;
						}
						throw new FastDfsException("connect to server " + inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort() + " fail, wait_time > " + ClientGlobal.g_connection_pool_max_wait_time_in_ms + "ms");
					} catch (InterruptedException e) {
						logger.error("建立连接异常", e);
						throw new FastDfsException("connect to server " + inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort() + " fail, emsg:" + e.getMessage());
					}
				}
				return connection;
			}
		} finally {
			lock.unlock();
		}
	}

	public void releaseConnection(Connection connection) {
		if (connection == null) {
			return;
		}
		lock.lock();
		try {
			connection.setLastAccessTime(System.currentTimeMillis());
			freeConnections.add(connection);
			freeCount.incrementAndGet();
			condition.signal();
		} finally {
			lock.unlock();
		}
	}

	public void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				totalCount.decrementAndGet();
				connection.closeDirectly();
			}
		} catch (IOException e) {
			logger.error("close socket[{}:{}] error.",
					inetSocketAddress.getAddress().getHostAddress(),
					inetSocketAddress.getPort(), e);
		}
	}

	public void setActiveTestFlag() {
		if (freeCount.get() > 0) {
			lock.lock();
			try {
				for (Connection freeConnection : freeConnections) {
					freeConnection.setNeedActiveTest(true);
				}
			} finally {
				lock.unlock();
			}
		}
	}

	@Override
	public String toString() {
		return "ConnectionManager{" +
				"ip:port='" + inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort() +
				", totalCount=" + totalCount +
				", freeCount=" + freeCount +
				", freeConnections =" + freeConnections +
				'}';
	}
}
