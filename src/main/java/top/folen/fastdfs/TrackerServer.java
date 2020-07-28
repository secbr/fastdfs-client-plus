package top.folen.fastdfs;

import top.folen.common.FastDfsException;
import top.folen.fastdfs.pool.Connection;
import top.folen.fastdfs.pool.ConnectionPool;
import top.folen.fastdfs.pool.ConnectionUtil;

import java.net.InetSocketAddress;

/**
 * Tracker Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class TrackerServer {

	protected InetSocketAddress inetSockAddr;

	public TrackerServer(InetSocketAddress inetSockAddr) {
		this.inetSockAddr = inetSockAddr;
	}

	public Connection getConnection() throws FastDfsException {
		if (ClientGlobal.g_connection_pool_enabled) {
			return ConnectionPool.getConnection(this.inetSockAddr);
		} else {
			return ConnectionUtil.create(this.inetSockAddr);
		}
	}

	/**
	 * get the server info
	 *
	 * @return the server info
	 */
	public InetSocketAddress getInetSocketAddress() {
		return this.inetSockAddr;
	}

}
