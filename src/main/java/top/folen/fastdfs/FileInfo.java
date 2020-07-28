package top.folen.fastdfs;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Server Info
 *
 * @author Happy Fish / YuQing
 * @version Version 1.23
 */
public class FileInfo {

	public static final short FILE_TYPE_NORMAL = 1;
	public static final short FILE_TYPE_APPENDER = 2;
	public static final short FILE_TYPE_SLAVE = 4;

	protected boolean fetchFromServer;

	protected short fileType;

	protected String sourceIpAddr;

	protected long fileSize;

	protected Date createTimestamp;

	protected int crc32;

	/**
	 * Constructor
	 *
	 * @param fetchFromServer if fetch from server flag
	 * @param fileType         the file type
	 * @param fileSize         the file size
	 * @param createTimestamp  create timestamp in seconds
	 * @param crc32             the crc32 signature
	 * @param sourceIpAddr    the source storage ip address
	 */
	public FileInfo(boolean fetchFromServer, short fileType, long fileSize,
	                int createTimestamp, int crc32, String sourceIpAddr) {
		this.fetchFromServer = fetchFromServer;
		this.fileType = fileType;
		this.fileSize = fileSize;
		this.createTimestamp = new Date(createTimestamp * 1000L);
		this.crc32 = crc32;
		this.sourceIpAddr = sourceIpAddr;
	}

	/**
	 * get the fetch_from_server flag
	 *
	 * @return the fetch_from_server flag
	 */
	public boolean getFetchFromServer() {
		return this.fetchFromServer;
	}

	/**
	 * set the fetch_from_server flag
	 *
	 * @param fetch_from_server the fetch from server flag
	 */
	public void setFetchFromServer(boolean fetch_from_server) {
		this.fetchFromServer = fetch_from_server;
	}

	/**
	 * get the file type
	 *
	 * @return the file type
	 */
	public short getFileType() {
		return this.fileType;
	}

	/**
	 * set the file type
	 *
	 * @param file_type the file type
	 */
	public void setFileType(short file_type) {
		this.fileType = file_type;
	}

	/**
	 * get the source ip address of the file uploaded to
	 *
	 * @return the source ip address of the file uploaded to
	 */
	public String getSourceIpAddr() {
		return this.sourceIpAddr;
	}

	/**
	 * set the source ip address of the file uploaded to
	 *
	 * @param source_ip_addr the source ip address
	 */
	public void setSourceIpAddr(String source_ip_addr) {
		this.sourceIpAddr = source_ip_addr;
	}

	/**
	 * get the file size
	 *
	 * @return the file size
	 */
	public long getFileSize() {
		return this.fileSize;
	}

	/**
	 * set the file size
	 *
	 * @param file_size the file size
	 */
	public void setFileSize(long file_size) {
		this.fileSize = file_size;
	}

	/**
	 * get the create timestamp of the file
	 *
	 * @return the create timestamp of the file
	 */
	public Date getCreateTimestamp() {
		return this.createTimestamp;
	}

	/**
	 * set the create timestamp of the file
	 *
	 * @param create_timestamp create timestamp in seconds
	 */
	public void setCreateTimestamp(int create_timestamp) {
		this.createTimestamp = new Date(create_timestamp * 1000L);
	}

	/**
	 * get the file CRC32 signature
	 *
	 * @return the file CRC32 signature
	 */
	public long getCrc32() {
		return this.crc32;
	}

	/**
	 * set the create timestamp of the file
	 *
	 * @param crc32 the crc32 signature
	 */
	public void setCrc32(int crc32) {
		this.crc32 = crc32;
	}

	/**
	 * to string
	 *
	 * @return string
	 */
	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "fetch_from_server = " + this.fetchFromServer + ", " +
				"file_type = " + this.fileType + ", " +
				"source_ip_addr = " + this.sourceIpAddr + ", " +
				"file_size = " + this.fileSize + ", " +
				"create_timestamp = " + df.format(this.createTimestamp) + ", " +
				"crc32 = " + this.crc32;
	}
}
