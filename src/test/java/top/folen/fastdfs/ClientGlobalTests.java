package top.folen.fastdfs;

import org.junit.Test;
import top.folen.common.FastDfsException;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by zzs
 */
public class ClientGlobalTests {

	@Test
	public void testInitByTrackers() throws IOException, FastDfsException {
		String trackerServers = "10.0.11.101:22122,10.0.11.102:22122";
		ClientGlobal.initByTrackers(trackerServers);
		System.out.println("ClientGlobal.configInfo() : " + ClientGlobal.configInfo());
	}

	@Test
	public void testInitByProperties() throws IOException, FastDfsException {
		String propFilePath = "fastdfs-client.properties";
		ClientGlobal.initByProperties(propFilePath);
		System.out.println("ClientGlobal.configInfo() : " + ClientGlobal.configInfo());
	}

	@Test
	public void testInitByPropertiesAndServers() throws IOException, FastDfsException {
		Properties props = new Properties();
		props.put("fastdfs.tracker_servers", "10.0.11.101:22122,10.0.11.102:22122");
		ClientGlobal.initByProperties(props);
		System.out.println("ClientGlobal.configInfo(): " + ClientGlobal.configInfo());
	}

	@Test
	public void testInit() throws FastDfsException {
		ClientGlobal.init("fdfs_client.conf");
		System.out.println(ClientGlobal.G_TRACKER_GROUP.getTrackerServer().inetSockAddr.getAddress());
	}

}
