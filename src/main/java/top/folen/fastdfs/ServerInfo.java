package top.folen.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.7
 */
public class ServerInfo {

	protected String ipAddr;

	protected int port;

	/**
	 * Constructor
	 *
	 * @param ipAddr address of the server
	 * @param port   the port of the server
	 */
	public ServerInfo(String ipAddr, int port) {
		this.ipAddr = ipAddr;
		this.port = port;
	}

	/**
	 * return the ip address
	 *
	 * @return the ip address
	 */
	public String getIpAddr() {
		return this.ipAddr;
	}

	/**
	 * return the port of the server
	 *
	 * @return the port of the server
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * connect to server
	 *
	 * @return connected Socket object
	 */
	public Socket connect() throws IOException {
		Socket sock = new Socket();
		sock.setReuseAddress(true);
		sock.setSoTimeout(ClientGlobal.g_network_timeout);
		sock.connect(new InetSocketAddress(this.ipAddr, this.port), ClientGlobal.g_connect_timeout);
		return sock;
	}
}
