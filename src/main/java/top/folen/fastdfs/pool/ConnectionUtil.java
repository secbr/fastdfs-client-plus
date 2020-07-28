package top.folen.fastdfs.pool;

import top.folen.common.FastDfsException;
import top.folen.fastdfs.ClientGlobal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Connection 工具类，提供Connection的创建、关闭和释放操作
 *
 * @author sec
 * @version 1.0.1
 **/
public class ConnectionUtil {

	/**
	 * 根据InetSocketAddress创建连接
	 *
	 * @param socketAddress InetSocketAddress
	 * @return 返回连接信息
	 * @throws FastDfsException
	 */
	public static Connection create(InetSocketAddress socketAddress) throws FastDfsException {
		try {
			Socket sock = new Socket();
			sock.setReuseAddress(true);
			sock.setSoTimeout(ClientGlobal.g_network_timeout);
			sock.connect(socketAddress, ClientGlobal.g_connect_timeout);
			return new Connection(sock, socketAddress);
		} catch (Exception e) {
			throw new FastDfsException("connect to server " + socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort() + " fail, emsg:" + e.getMessage());
		}
	}

	/**
	 * 关闭连接的统一方法
	 *
	 * @param connection 连接
	 */
	public static void close(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			connection = null;
		}
	}

	/**
	 * 释放连接
	 *
	 * @param connection 连接
	 */
	public static void release(Connection connection) {
		if (connection != null) {
			try {
				connection.release();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
