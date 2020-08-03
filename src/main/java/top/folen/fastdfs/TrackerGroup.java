package top.folen.fastdfs;

import java.net.InetSocketAddress;

/**
 * Tracker server group
 *
 * @author Happy Fish / YuQing
 * @version Version 1.17
 */
public class TrackerGroup {

	public int trackerServerIndex;

	public InetSocketAddress[] trackerServers;

	protected final Integer lock;

	/**
	 * Constructor
	 *
	 * @param trackerServers tracker servers
	 */
	public TrackerGroup(InetSocketAddress[] trackerServers) {
		this.trackerServers = trackerServers;
		this.lock = 0;
		this.trackerServerIndex = 0;
	}

	/**
	 * return connected tracker server
	 *
	 * @return connected tracker server, null for fail
	 */
	public TrackerServer getTrackerServer(int serverIndex) {
		return new TrackerServer(this.trackerServers[serverIndex]);
	}

	/**
	 * return connected tracker server
	 *
	 * @return connected tracker server, null for fail
	 */
	public TrackerServer getTrackerServer() {
		if (trackerServers == null || trackerServers.length == 0) {
			return null;
		}
		synchronized (this.lock) {
			this.trackerServerIndex++;
			if (this.trackerServerIndex >= this.trackerServers.length) {
				this.trackerServerIndex = 0;
			}
		}
		return this.getTrackerServer(trackerServerIndex);
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		super.clone();
		InetSocketAddress[] trackerServers = new InetSocketAddress[this.trackerServers.length];
		for (int i = 0; i < trackerServers.length; i++) {
			trackerServers[i] = new InetSocketAddress(this.trackerServers[i].getAddress().getHostAddress(),
					this.trackerServers[i].getPort());
		}

		return new TrackerGroup(trackerServers);
	}
}
