package top.folen.fastdfs;

import java.io.IOException;
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
	public TrackerServer getTrackerServer(int serverIndex) throws IOException {
		return new TrackerServer(this.trackerServers[serverIndex]);
	}

	/**
	 * return connected tracker server
	 *
	 * @return connected tracker server, null for fail
	 */
	public TrackerServer getTrackerServer() {
		int currentIndex;

		synchronized (this.lock) {
			this.trackerServerIndex++;
			if (this.trackerServerIndex >= this.trackerServers.length) {
				this.trackerServerIndex = 0;
			}

			currentIndex = this.trackerServerIndex;
		}

		try {
			return this.getTrackerServer(currentIndex);
		} catch (IOException ex) {
			System.err.println("connect to server " + this.trackerServers[currentIndex].getAddress().getHostAddress() + ":" + this.trackerServers[currentIndex].getPort() + " fail");
			ex.printStackTrace(System.err);
		}

		for (int i = 0; i < this.trackerServers.length; i++) {
			if (i == currentIndex) {
				continue;
			}

			try {
				TrackerServer trackerServer = this.getTrackerServer(i);

				synchronized (this.lock) {
					if (this.trackerServerIndex == currentIndex) {
						this.trackerServerIndex = i;
					}
				}
				return trackerServer;
			} catch (IOException ex) {
				System.err.println("connect to server " + this.trackerServers[i].getAddress().getHostAddress() + ":" + this.trackerServers[i].getPort() + " fail");
				ex.printStackTrace(System.err);
			}
		}

		return null;
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
