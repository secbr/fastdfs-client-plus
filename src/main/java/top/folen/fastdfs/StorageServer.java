package top.folen.fastdfs;

import java.net.InetSocketAddress;

/**
 * Storage Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class StorageServer extends TrackerServer {

	protected int storePathIndex;

	/**
	 * Constructor
	 *
	 * @param ipAddr    the ip address of storage server
	 * @param port      the port of storage server
	 * @param storePath the store path index on the storage server
	 */
	public StorageServer(String ipAddr, int port, int storePath) {
		super(new InetSocketAddress(ipAddr, port));
		this.storePathIndex = storePath;
	}

	/**
	 * Constructor
	 *
	 * @param ipAddr    the ip address of storage server
	 * @param port      the port of storage server
	 * @param storePath the store path index on the storage server
	 */
	public StorageServer(String ipAddr, int port, byte storePath) {
		super(new InetSocketAddress(ipAddr, port));
		if (storePath < 0) {
			this.storePathIndex = 256 + storePath;
		} else {
			this.storePathIndex = storePath;
		}
	}

	/**
	 * @return the store path index on the storage server
	 */
	public int getStorePathIndex() {
		return this.storePathIndex;
	}
}
