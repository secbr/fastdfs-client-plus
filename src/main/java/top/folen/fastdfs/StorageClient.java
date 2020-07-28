package top.folen.fastdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.folen.common.Base64;
import top.folen.common.FastDfsException;
import top.folen.common.NameValuePair;
import top.folen.fastdfs.pool.Connection;
import top.folen.fastdfs.pool.ConnectionUtil;

import java.io.*;
import java.util.Arrays;

/**
 * Storage client for 2 fields file id: group name and filename
 * Note: the instance of this class is NOT thread safe !!!
 * if not necessary, do NOT set storage server instance
 *
 * @author Happy Fish / YuQing
 * @version Version 1.27
 */
public class StorageClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient.class);

	public final static Base64 base64 = new Base64('-', '_', '.', 0);
	protected TrackerServer trackerServer;
	protected StorageServer storageServer;
	protected byte errno;

	/**
	 * constructor using global settings in class ClientGlobal
	 */
	public StorageClient() {
		this.trackerServer = null;
		this.storageServer = null;
	}

	/**
	 * constructor with tracker server
	 *
	 * @param trackerServer the tracker server, can be null
	 */
	public StorageClient(TrackerServer trackerServer) {
		this.trackerServer = trackerServer;
		this.storageServer = null;
	}

	/**
	 * constructor with tracker server and storage server
	 * NOTE: if not necessary, do NOT set storage server instance
	 *
	 * @param trackerServer the tracker server, can be null
	 * @param storageServer the storage server, can be null
	 */
	public StorageClient(TrackerServer trackerServer, StorageServer storageServer) {
		this.trackerServer = trackerServer;
		this.storageServer = storageServer;
	}

	/**
	 * get the error code of last call
	 *
	 * @return the error code of last call
	 */
	public byte getErrorCode() {
		return this.errno;
	}

	/**
	 * upload file to storage server (by file name)
	 *
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String localFilename, String fileExtName,
	                           NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadFile(null, localFilename, fileExtName, metaList);
	}

	/**
	 * upload file to storage server (by file name)
	 *
	 * @param groupName     the group name to upload file to, can be empty
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	protected String[] uploadFile(String groupName, String localFilename, String fileExtName,
	                              NameValuePair[] metaList) throws IOException, FastDfsException {
		final byte cmd = ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE;
		return this.uploadFile(cmd, groupName, localFilename, fileExtName, metaList);
	}

	/**
	 * upload file to storage server (by file name)
	 *
	 * @param cmd           the command
	 * @param groupName     the group name to upload file to, can be empty
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	protected String[] uploadFile(byte cmd, String groupName, String localFilename, String fileExtName,
	                              NameValuePair[] metaList) throws IOException, FastDfsException {
		File f = new File(localFilename);
		FileInputStream fis = new FileInputStream(f);

		if (fileExtName == null) {
			int nPos = localFilename.lastIndexOf('.');
			if (nPos > 0 && localFilename.length() - nPos <= ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
				fileExtName = localFilename.substring(nPos + 1);
			}
		}

		try {
			return this.doUploadFile(cmd, groupName, null, null, fileExtName,
					f.length(), new UploadStream(fis, f.length()), metaList);
		} finally {
			fis.close();
		}
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param offset      start offset of the buff
	 * @param length      the length of buff to upload
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(byte[] fileBuff, int offset, int length, String fileExtName,
	                           NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadFile(null, fileBuff, offset, length, fileExtName, metaList);
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param offset      start offset of the buff
	 * @param length      the length of buff to upload
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, byte[] fileBuff, int offset, int length,
	                           String fileExtName, NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, null,
				null, fileExtName, length, new UploadBuff(fileBuff, offset, length), metaList);
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(byte[] fileBuff, String fileExtName,
	                           NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadFile(null, fileBuff, 0, fileBuff.length, fileExtName, metaList);
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, byte[] fileBuff,
	                           String fileExtName, NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName, null,
				null, fileExtName, fileBuff.length,
				new UploadBuff(fileBuff, 0, fileBuff.length), metaList);
	}

	/**
	 * upload file to storage server (by callback)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileSize    the file size
	 * @param callback    the write data callback object
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, long fileSize, UploadCallback callback,
	                           String fileExtName, NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_FILE, groupName,
				null, null, fileExtName, fileSize, callback, metaList);
	}

	/**
	 * upload file to storage server (by file name, slave file mode)
	 *
	 * @param groupName      the group name of master file
	 * @param masterFilename the master file name to generate the slave file
	 * @param prefixName     the prefix name to generate the slave file
	 * @param localFilename  local filename to upload
	 * @param fileExtName    file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList       meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, String masterFilename, String prefixName,
	                           String localFilename, String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		if ((groupName == null || groupName.length() == 0) ||
				(masterFilename == null || masterFilename.length() == 0) ||
				(prefixName == null)) {
			throw new FastDfsException("invalid arguement");
		}

		File f = new File(localFilename);
		FileInputStream fis = new FileInputStream(f);

		if (fileExtName == null) {
			int nPos = localFilename.lastIndexOf('.');
			if (nPos > 0 && localFilename.length() - nPos <= ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
				fileExtName = localFilename.substring(nPos + 1);
			}
		}

		try {
			return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename,
					prefixName, fileExtName, f.length(), new UploadStream(fis, f.length()), metaList);
		} finally {
			fis.close();
		}
	}

	/**
	 * upload file to storage server (by file buff, slave file mode)
	 *
	 * @param groupName      the group name of master file
	 * @param masterFilename the master file name to generate the slave file
	 * @param prefixName     the prefix name to generate the slave file
	 * @param fileBuff       file content/buff
	 * @param fileExtName    file ext name, do not include dot(.)
	 * @param metaList       meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, String masterFilename, String prefixName,
	                           byte[] fileBuff, String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		if ((groupName == null || groupName.length() == 0) ||
				(masterFilename == null || masterFilename.length() == 0) ||
				(prefixName == null)) {
			throw new FastDfsException("invalid arguement");
		}

		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename,
				prefixName, fileExtName, fileBuff.length,
				new UploadBuff(fileBuff, 0, fileBuff.length), metaList);
	}

	/**
	 * upload file to storage server (by file buff, slave file mode)
	 *
	 * @param groupName      the group name of master file
	 * @param masterFilename the master file name to generate the slave file
	 * @param prefixName     the prefix name to generate the slave file
	 * @param fileBuff       file content/buff
	 * @param offset         start offset of the buff
	 * @param length         the length of buff to upload
	 * @param fileExtName    file ext name, do not include dot(.)
	 * @param metaList       meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, String masterFilename, String prefixName,
	                           byte[] fileBuff, int offset, int length, String fileExtName,
	                           NameValuePair[] metaList) throws IOException, FastDfsException {
		if ((groupName == null || groupName.length() == 0) ||
				(masterFilename == null || masterFilename.length() == 0) ||
				(prefixName == null)) {
			throw new FastDfsException("invalid arguement");
		}

		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName, masterFilename,
				prefixName, fileExtName, length, new UploadBuff(fileBuff, offset, length), metaList);
	}

	/**
	 * upload file to storage server (by callback, slave file mode)
	 *
	 * @param groupName      the group name to upload file to, can be empty
	 * @param masterFilename the master file name to generate the slave file
	 * @param prefixName     the prefix name to generate the slave file
	 * @param fileSize       the file size
	 * @param callback       the write data callback object
	 * @param fileExtName    file ext name, do not include dot(.)
	 * @param metaList       meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadFile(String groupName, String masterFilename,
	                           String prefixName, long fileSize, UploadCallback callback,
	                           String fileExtName, NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, groupName,
				masterFilename, prefixName, fileExtName, fileSize, callback, metaList);
	}

	/**
	 * upload appender file to storage server (by file name)
	 *
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(String localFilename, String fileExtName,
	                                   NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadAppenderFile(null, localFilename, fileExtName, metaList);
	}

	/**
	 * upload appender file to storage server (by file name)
	 *
	 * @param groupName     the group name to upload file to, can be empty
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file </li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	protected String[] uploadAppenderFile(String groupName, String localFilename, String fileExtName,
	                                      NameValuePair[] metaList) throws IOException, FastDfsException {
		final byte cmd = ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE;
		return this.uploadFile(cmd, groupName, localFilename, fileExtName, metaList);
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param offset      start offset of the buff
	 * @param length      the length of buff to upload
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(byte[] fileBuff, int offset, int length, String fileExtName,
	                                   NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadAppenderFile(null, fileBuff, offset, length, fileExtName, metaList);
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param offset      start offset of the buff
	 * @param length      the length of buff to upload
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(String groupName, byte[] fileBuff, int offset, int length,
	                                   String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, null, null,
				fileExtName, length, new UploadBuff(fileBuff, offset, length), metaList);
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(byte[] fileBuff, String fileExtName,
	                                   NameValuePair[] metaList) throws IOException, FastDfsException {
		return this.uploadAppenderFile(null, fileBuff, 0, fileBuff.length, fileExtName, metaList);
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param group_name  the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(String group_name, byte[] fileBuff,
	                                   String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, group_name, null, null,
				fileExtName, fileBuff.length, new UploadBuff(fileBuff, 0, fileBuff.length), metaList);
	}

	/**
	 * upload appender file to storage server (by callback)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileSize    the file size
	 * @param callback    the write data callback object
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li>results[0]: the group name to store the file</li></ul>
	 * <ul><li>results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] uploadAppenderFile(String groupName, long fileSize, UploadCallback callback,
	                                   String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		return this.doUploadFile(ProtoCommon.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, groupName, null,
				null, fileExtName, fileSize, callback, metaList);
	}

	/**
	 * append file to storage server (by file name)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param localFilename    local filename to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile(String groupName, String appenderFilename, String localFilename) throws IOException,
			FastDfsException {
		File f = new File(localFilename);
		FileInputStream fis = new FileInputStream(f);
		try {
			return this.doAppendFile(groupName, appenderFilename, f.length(), new UploadStream(fis, f.length()));
		} finally {
			fis.close();
		}
	}

	/**
	 * append file to storage server (by file buff)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileBuff         file content/buff
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile(String groupName, String appenderFilename, byte[] fileBuff) throws IOException,
			FastDfsException {
		return this.doAppendFile(groupName, appenderFilename, fileBuff.length, new UploadBuff(fileBuff, 0,
				fileBuff.length));
	}

	/**
	 * append file to storage server (by file buff)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileBuff         file content/buff
	 * @param offset           start offset of the buff
	 * @param length           the length of buff to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile(String groupName, String appenderFilename,
	                      byte[] fileBuff, int offset, int length) throws IOException, FastDfsException {
		return this.doAppendFile(groupName, appenderFilename, length, new UploadBuff(fileBuff, offset, length));
	}

	/**
	 * append file to storage server (by callback)
	 *
	 * @param groupName        the group name to append file to
	 * @param appenderFilename the appender filename
	 * @param fileSize         the file size
	 * @param callback         the write data callback object
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile(String groupName, String appenderFilename,
	                      long fileSize, UploadCallback callback) throws IOException, FastDfsException {
		return this.doAppendFile(groupName, appenderFilename, fileSize, callback);
	}

	/**
	 * modify appender file to storage server (by file name)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileOffset       the offset of appender file
	 * @param localFilename    local filename to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile(String groupName, String appenderFilename,
	                      long fileOffset, String localFilename) throws IOException, FastDfsException {
		File f = new File(localFilename);
		FileInputStream fis = new FileInputStream(f);

		try {
			return this.doModifyFile(groupName, appenderFilename, fileOffset,
					f.length(), new UploadStream(fis, f.length()));
		} finally {
			fis.close();
		}
	}

	/**
	 * modify appender file to storage server (by file buff)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileOffset       the offset of appender file
	 * @param fileBuff         file content/buff
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile(String groupName, String appenderFilename,
	                      long fileOffset, byte[] fileBuff) throws IOException, FastDfsException {
		return this.doModifyFile(groupName, appenderFilename, fileOffset,
				fileBuff.length, new UploadBuff(fileBuff, 0, fileBuff.length));
	}

	/**
	 * modify appender file to storage server (by file buff)
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileOffset       the offset of appender file
	 * @param fileBuff         file content/buff
	 * @param bufferOffset     start offset of the buff
	 * @param bufferLength     the length of buff to modify
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile(String groupName, String appenderFilename,
	                      long fileOffset, byte[] fileBuff, int bufferOffset, int bufferLength) throws IOException,
			FastDfsException {
		return this.doModifyFile(groupName, appenderFilename, fileOffset,
				bufferLength, new UploadBuff(fileBuff, bufferOffset, bufferLength));
	}

	/**
	 * modify appender file to storage server (by callback)
	 *
	 * @param groupName        the group name to modify file to
	 * @param appenderFilename the appender filename
	 * @param fileOffset       the offset of appender file
	 * @param modifySize       the modify size
	 * @param callback         the write data callback object
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile(String groupName, String appenderFilename,
	                      long fileOffset, long modifySize, UploadCallback callback) throws IOException,
			FastDfsException {
		return this.doModifyFile(groupName, appenderFilename, fileOffset,
				modifySize, callback);
	}

	/**
	 * regenerate filename for appender file
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @return 2 elements string array if success:<br>
	 * <ul><li> results[0]: the group name to store the file</li></ul>
	 * <ul><li> results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	public String[] regenerateAppenderFilename(String groupName, String appenderFilename) throws IOException,
			FastDfsException {
		byte[] header;
		boolean bNewStorageServer;
		Connection connection = null;
		byte[] appenderFilenameBytes;
		int offset;
		long bodyLen;

		if ((groupName == null || groupName.length() == 0) ||
				(appenderFilename == null || appenderFilename.length() == 0)) {
			this.errno = ProtoCommon.ERR_NO_EINVAL;
			return null;
		}

		bNewStorageServer = this.newUpdatableStorageConnection(groupName, appenderFilename);

		try {
			connection = this.storageServer.getConnection();

			appenderFilenameBytes = appenderFilename.getBytes(ClientGlobal.G_CHARSET);
			bodyLen = appenderFilenameBytes.length;

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_REGENERATE_APPENDER_FILENAME, bodyLen,
					(byte) 0);
			byte[] wholePkg = new byte[(int) (header.length + bodyLen)];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			offset = header.length;

			System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
			offset += appenderFilenameBytes.length;

			OutputStream out = connection.getOutputStream();
			out.write(wholePkg);

			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return null;
			}

			if (pkgInfo.body.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
				throw new FastDfsException("body length: " + pkgInfo.body.length + " <= " + ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
			}

			String new_group_name = new String(pkgInfo.body, 0, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN).trim();
			String remote_filename = new String(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN,
					pkgInfo.body.length - ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
			String[] results = new String[2];
			results[0] = new_group_name;
			results[1] = remote_filename;

			return results;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * upload file to storage server
	 *
	 * @param cmd            the command code
	 * @param groupName      the group name to upload file to, can be empty
	 * @param masterFilename the master file name to generate the slave file
	 * @param prefixName     the prefix name to generate the slave file
	 * @param fileExtName    file ext name, do not include dot(.)
	 * @param fileSize       the file size
	 * @param callback       the write data callback object
	 * @param metaList       meta info array
	 * @return 2 elements string array if success:<br>
	 * <ul><li> results[0]: the group name to store the file</li></ul>
	 * <ul><li> results[1]: the new created filename</li></ul>
	 * return null if fail
	 */
	protected String[] doUploadFile(byte cmd, String groupName, String masterFilename,
	                                String prefixName, String fileExtName, long fileSize,
	                                UploadCallback callback,
	                                NameValuePair[] metaList) throws IOException, FastDfsException {
		byte[] header;
		byte[] extNameBs;
		String newGroupName;
		String remoteFilename;
		boolean bNewStorageServer;
		Connection connection = null;
		byte[] sizeBytes;
		byte[] hexLenBytes;
		byte[] masterFilenameBytes;
		boolean bUploadSlave;
		int offset;
		long bodyLen;

		bUploadSlave = ((groupName != null && groupName.length() > 0) &&
				(masterFilename != null && masterFilename.length() > 0) &&
				(prefixName != null));
		if (bUploadSlave) {
			bNewStorageServer = this.newUpdatableStorageConnection(groupName, masterFilename);
		} else {
			bNewStorageServer = this.newWritableStorageConnection(groupName);
		}

		try {
			connection = this.storageServer.getConnection();

			extNameBs = new byte[ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN];
			Arrays.fill(extNameBs, (byte) 0);
			if (fileExtName != null && fileExtName.length() > 0) {
				byte[] bs = fileExtName.getBytes(ClientGlobal.G_CHARSET);
				int ext_name_len = bs.length;
				if (ext_name_len > ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN) {
					ext_name_len = ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN;
				}
				System.arraycopy(bs, 0, extNameBs, 0, ext_name_len);
			}

			if (bUploadSlave) {
				masterFilenameBytes = masterFilename.getBytes(ClientGlobal.G_CHARSET);

				sizeBytes = new byte[2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
				bodyLen =
						sizeBytes.length + ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN
								+ masterFilenameBytes.length + fileSize;

				hexLenBytes = ProtoCommon.long2buff(masterFilename.length());
				System.arraycopy(hexLenBytes, 0, sizeBytes, 0, hexLenBytes.length);
				offset = hexLenBytes.length;
			} else {
				masterFilenameBytes = null;
				sizeBytes = new byte[1 + ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
				bodyLen = sizeBytes.length + ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + fileSize;

				sizeBytes[0] = (byte) this.storageServer.getStorePathIndex();
				offset = 1;
			}

			hexLenBytes = ProtoCommon.long2buff(fileSize);
			System.arraycopy(hexLenBytes, 0, sizeBytes, offset, hexLenBytes.length);

			OutputStream out = connection.getOutputStream();
			header = ProtoCommon.packHeader(cmd, bodyLen, (byte) 0);
			byte[] wholePkg = new byte[(int) (header.length + bodyLen - fileSize)];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
			offset = header.length + sizeBytes.length;
			if (bUploadSlave) {
				byte[] prefix_name_bs = new byte[ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN];
				byte[] bs = prefixName.getBytes(ClientGlobal.G_CHARSET);
				int prefix_name_len = bs.length;
				Arrays.fill(prefix_name_bs, (byte) 0);
				if (prefix_name_len > ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN) {
					prefix_name_len = ProtoCommon.FDFS_FILE_PREFIX_MAX_LEN;
				}
				if (prefix_name_len > 0) {
					System.arraycopy(bs, 0, prefix_name_bs, 0, prefix_name_len);
				}

				System.arraycopy(prefix_name_bs, 0, wholePkg, offset, prefix_name_bs.length);
				offset += prefix_name_bs.length;
			}

			System.arraycopy(extNameBs, 0, wholePkg, offset, extNameBs.length);
			offset += extNameBs.length;

			if (bUploadSlave) {
				System.arraycopy(masterFilenameBytes, 0, wholePkg, offset, masterFilenameBytes.length);
				offset += masterFilenameBytes.length;
			}

			out.write(wholePkg);

			if ((this.errno = (byte) callback.send(out)) != 0) {
				return null;
			}

			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return null;
			}

			if (pkgInfo.body.length <= ProtoCommon.FDFS_GROUP_NAME_MAX_LEN) {
				throw new FastDfsException("body length: " + pkgInfo.body.length + " <= " + ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
			}

			newGroupName = new String(pkgInfo.body, 0, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN).trim();
			remoteFilename = new String(pkgInfo.body, ProtoCommon.FDFS_GROUP_NAME_MAX_LEN,
					pkgInfo.body.length - ProtoCommon.FDFS_GROUP_NAME_MAX_LEN);
			String[] results = new String[2];
			results[0] = newGroupName;
			results[1] = remoteFilename;

			if (metaList == null || metaList.length == 0) {
				return results;
			}

			int result = 0;
			try {
				result = this.setMetadata(newGroupName, remoteFilename,
						metaList, ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE);
			} catch (IOException ex) {
				result = 5;
				throw ex;
			} finally {
				if (result != 0) {
					this.errno = (byte) result;
					this.deleteFile(newGroupName, remoteFilename);
					return null;
				}
			}
			return results;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * append file to storage server
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileSize         the file size
	 * @param callback         the write data callback object
	 * @return return true for success, false for fail
	 */
	protected int doAppendFile(String groupName, String appenderFilename,
	                           long fileSize, UploadCallback callback) throws IOException, FastDfsException {
		byte[] header;
		boolean bNewStorageServer;
		Connection connection = null;
		byte[] hexLenBytes;
		byte[] appenderFilenameBytes;
		int offset;
		long bodyLen;

		if ((groupName == null || groupName.length() == 0) ||
				(appenderFilename == null || appenderFilename.length() == 0)) {
			this.errno = ProtoCommon.ERR_NO_EINVAL;
			return this.errno;
		}

		bNewStorageServer = this.newUpdatableStorageConnection(groupName, appenderFilename);

		try {
			connection = this.storageServer.getConnection();

			appenderFilenameBytes = appenderFilename.getBytes(ClientGlobal.G_CHARSET);
			bodyLen = 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length + fileSize;

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_APPEND_FILE, bodyLen, (byte) 0);
			byte[] wholePkg = new byte[(int) (header.length + bodyLen - fileSize)];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			offset = header.length;

			hexLenBytes = ProtoCommon.long2buff(appenderFilename.length());
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			hexLenBytes = ProtoCommon.long2buff(fileSize);
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			OutputStream out = connection.getOutputStream();

			System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
			offset += appenderFilenameBytes.length;

			out.write(wholePkg);
			if ((this.errno = (byte) callback.send(out)) != 0) {
				return this.errno;
			}

			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return this.errno;
			}

			return 0;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	private void releaseConnection(Connection connection, boolean bNewStorageServer) {
		try {
			if (connection != null) {
				connection.release();
			}
		} catch (IOException e) {
			LOGGER.error("释放连接异常", e);
		} finally {
			if (bNewStorageServer) {
				this.storageServer = null;
			}
		}
	}

	/**
	 * modify appender file to storage server
	 *
	 * @param groupName        the group name of appender file
	 * @param appenderFilename the appender filename
	 * @param fileOffset       the offset of appender file
	 * @param modifySize       the modify size
	 * @param callback         the write data callback object
	 * @return return true for success, false for fail
	 */
	protected int doModifyFile(String groupName, String appenderFilename,
	                           long fileOffset, long modifySize, UploadCallback callback) throws IOException,
			FastDfsException {
		byte[] header;
		boolean bNewStorageServer;
		Connection connection = null;
		byte[] hexLenBytes;
		byte[] appenderFilenameBytes;
		int offset;
		long bodyLen;

		if ((groupName == null || groupName.length() == 0) ||
				(appenderFilename == null || appenderFilename.length() == 0)) {
			this.errno = ProtoCommon.ERR_NO_EINVAL;
			return this.errno;
		}

		bNewStorageServer = this.newUpdatableStorageConnection(groupName, appenderFilename);

		try {
			connection = this.storageServer.getConnection();

			appenderFilenameBytes = appenderFilename.getBytes(ClientGlobal.G_CHARSET);
			bodyLen = 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length + modifySize;

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_MODIFY_FILE, bodyLen, (byte) 0);
			byte[] wholePkg = new byte[(int) (header.length + bodyLen - modifySize)];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			offset = header.length;

			hexLenBytes = ProtoCommon.long2buff(appenderFilename.length());
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			hexLenBytes = ProtoCommon.long2buff(fileOffset);
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			hexLenBytes = ProtoCommon.long2buff(modifySize);
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			OutputStream out = connection.getOutputStream();

			System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
			offset += appenderFilenameBytes.length;

			out.write(wholePkg);
			if ((this.errno = (byte) callback.send(out)) != 0) {
				return this.errno;
			}

			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return this.errno;
			}

			return 0;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * delete file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int deleteFile(String groupName, String remoteFilename) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newUpdatableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();

		try {
			this.sendPackage(ProtoCommon.STORAGE_PROTO_CMD_DELETE_FILE, groupName, remoteFilename, connection);
			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);

			this.errno = pkgInfo.errno;
			return pkgInfo.errno;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * truncate appender file to size 0 from storage server
	 *
	 * @param groupName        the group name of storage server
	 * @param appenderFilename the appender filename
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int truncateFile(String groupName, String appenderFilename) throws FastDfsException, IOException {
		return this.truncateFile(groupName, appenderFilename, 0);
	}

	/**
	 * truncate appender file from storage server
	 *
	 * @param groupName         the group name of storage server
	 * @param appenderFilename  the appender filename
	 * @param truncatedFileSize truncated file size
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int truncateFile(String groupName, String appenderFilename,
	                        long truncatedFileSize) throws IOException, FastDfsException {
		byte[] header;
		boolean bNewStorageServer;
		Connection connection = null;
		byte[] hexLenBytes;
		byte[] appenderFilenameBytes;
		int offset;
		int bodyLen;

		if ((groupName == null || groupName.length() == 0) ||
				(appenderFilename == null || appenderFilename.length() == 0)) {
			this.errno = ProtoCommon.ERR_NO_EINVAL;
			return this.errno;
		}

		bNewStorageServer = this.newUpdatableStorageConnection(groupName, appenderFilename);

		try {
			connection = this.storageServer.getConnection();

			appenderFilenameBytes = appenderFilename.getBytes(ClientGlobal.G_CHARSET);
			bodyLen = 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + appenderFilenameBytes.length;

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_TRUNCATE_FILE, bodyLen, (byte) 0);
			byte[] wholePkg = new byte[header.length + bodyLen];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			offset = header.length;

			hexLenBytes = ProtoCommon.long2buff(appenderFilename.length());
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			hexLenBytes = ProtoCommon.long2buff(truncatedFileSize);
			System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
			offset += hexLenBytes.length;

			OutputStream out = connection.getOutputStream();

			System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
			offset += appenderFilenameBytes.length;

			out.write(wholePkg);
			ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);
			this.errno = pkgInfo.errno;
			return pkgInfo.errno;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return file content/buff, return null if fail
	 */
	public byte[] downloadFile(String groupName, String remoteFilename) throws IOException, FastDfsException {
		return this.downloadFile(groupName, remoteFilename, 0, 0);
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param fileOffset     the start offset of the file
	 * @param downloadBytes  download bytes, 0 for remain bytes from offset
	 * @return file content/buff, return null if fail
	 */
	public byte[] downloadFile(String groupName, String remoteFilename, long fileOffset, long downloadBytes) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newReadableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();

		try {
			ProtoCommon.RecvPackageInfo pkgInfo;

			this.sendDownloadPackage(groupName, remoteFilename, fileOffset, downloadBytes, connection);
			pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);

			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return null;
			}

			return pkgInfo.body;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param localFilename  filename on local
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile(String groupName, String remoteFilename,
	                        String localFilename) throws IOException, FastDfsException {
		return this.downloadFile(groupName, remoteFilename,
				0, 0, localFilename);
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param fileOffset     the start offset of the file
	 * @param downloadBytes  download bytes, 0 for remain bytes from offset
	 * @param localFilename  filename on local
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile(String groupName, String remoteFilename,
	                        long fileOffset, long downloadBytes,
	                        String localFilename) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newReadableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();
		try {
			ProtoCommon.RecvHeaderInfo header;
			FileOutputStream out = new FileOutputStream(localFilename);
			try {
				this.errno = 0;
				this.sendDownloadPackage(groupName, remoteFilename, fileOffset, downloadBytes, connection);

				InputStream in = connection.getInputStream();
				header = ProtoCommon.recvHeader(in, ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
				this.errno = header.errno;
				if (header.errno != 0) {
					return header.errno;
				}

				byte[] buff = new byte[256 * 1024];
				long remainBytes = header.bodyLen;
				int bytes;

				while (remainBytes > 0) {
					if ((bytes = in.read(buff, 0, remainBytes > buff.length ? buff.length : (int) remainBytes)) < 0) {
						throw new IOException("recv package size " + (header.bodyLen - remainBytes) + " != " + header.bodyLen);
					}

					out.write(buff, 0, bytes);
					remainBytes -= bytes;
				}

				return 0;
			} catch (IOException ex) {
				if (this.errno == 0) {
					this.errno = ProtoCommon.ERR_NO_EIO;
				}

				throw ex;
			} finally {
				out.close();
				if (this.errno != 0) {
					(new File(localFilename)).delete();
				}
			}
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param callback       call callback.recv() when data arrive
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile(String groupName, String remoteFilename,
	                        DownloadCallback callback) throws IOException, FastDfsException {
		return this.downloadFile(groupName, remoteFilename, 0, 0, callback);
	}

	/**
	 * download file from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param fileOffset     the start offset of the file
	 * @param downloadBytes  download bytes, 0 for remain bytes from offset
	 * @param callback       call callback.recv() when data arrive
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile(String groupName, String remoteFilename,
	                        long fileOffset, long downloadBytes,
	                        DownloadCallback callback) throws IOException, FastDfsException {
		int result;
		boolean bNewStorageServer = this.newReadableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();

		try {
			ProtoCommon.RecvHeaderInfo header;
			this.sendDownloadPackage(groupName, remoteFilename, fileOffset, downloadBytes, connection);

			InputStream in = connection.getInputStream();
			header = ProtoCommon.recvHeader(in, ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);
			this.errno = header.errno;
			if (header.errno != 0) {
				return header.errno;
			}

			byte[] buff = new byte[2 * 1024];
			long remainBytes = header.bodyLen;
			int bytes;

			while (remainBytes > 0) {
				if ((bytes = in.read(buff, 0, remainBytes > buff.length ? buff.length : (int) remainBytes)) < 0) {
					throw new IOException("recv package size " + (header.bodyLen - remainBytes) + " != " + header.bodyLen);
				}

				if ((result = callback.recv(header.bodyLen, buff, bytes)) != 0) {
					this.errno = (byte) result;
					return result;
				}

				remainBytes -= bytes;
			}

			return 0;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * get all metadata items from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return meta info array, return null if fail
	 */
	public NameValuePair[] getMetadata(String groupName, String remoteFilename) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newUpdatableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();
		try {
			ProtoCommon.RecvPackageInfo pkgInfo;

			this.sendPackage(ProtoCommon.STORAGE_PROTO_CMD_GET_METADATA, groupName, remoteFilename, connection);
			pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, -1);

			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return null;
			}

			return ProtoCommon.splitMetadata(new String(pkgInfo.body, ClientGlobal.G_CHARSET));
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * set metadata items to storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param metaList       meta item array
	 * @param opFlag         flag, can be one of following values: <br>
	 *                       <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE: overwrite all old
	 *                       metadata items</li></ul>
	 *                       <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_MERGE: merge, insert when
	 *                       the metadata item not exist, otherwise update it</li></ul>
	 * @return 0 for success, !=0 fail (error code)
	 */
	public int setMetadata(String groupName, String remoteFilename,
	                       NameValuePair[] metaList, byte opFlag) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newUpdatableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();
		try {
			byte[] header;
			byte[] groupBytes;
			byte[] filenameBytes;
			byte[] meta_buff;
			byte[] bs;
			int groupLen;
			byte[] sizeBytes;
			ProtoCommon.RecvPackageInfo pkgInfo;

			if (metaList == null) {
				meta_buff = new byte[0];
			} else {
				meta_buff = ProtoCommon.packMetadata(metaList).getBytes(ClientGlobal.G_CHARSET);
			}

			filenameBytes = remoteFilename.getBytes(ClientGlobal.G_CHARSET);
			sizeBytes = new byte[2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE];
			Arrays.fill(sizeBytes, (byte) 0);

			bs = ProtoCommon.long2buff(filenameBytes.length);
			System.arraycopy(bs, 0, sizeBytes, 0, bs.length);
			bs = ProtoCommon.long2buff(meta_buff.length);
			System.arraycopy(bs, 0, sizeBytes, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE, bs.length);

			groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
			bs = groupName.getBytes(ClientGlobal.G_CHARSET);

			Arrays.fill(groupBytes, (byte) 0);
			if (bs.length <= groupBytes.length) {
				groupLen = bs.length;
			} else {
				groupLen = groupBytes.length;
			}
			System.arraycopy(bs, 0, groupBytes, 0, groupLen);

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_SET_METADATA,
					2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE + 1 + groupBytes.length
							+ filenameBytes.length + meta_buff.length, (byte) 0);
			OutputStream out = connection.getOutputStream();
			byte[] wholePkg =
					new byte[header.length + sizeBytes.length + 1 + groupBytes.length + filenameBytes.length];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
			wholePkg[header.length + sizeBytes.length] = opFlag;
			System.arraycopy(groupBytes, 0, wholePkg, header.length + sizeBytes.length + 1, groupBytes.length);
			System.arraycopy(filenameBytes, 0, wholePkg, header.length + sizeBytes.length + 1 + groupBytes.length,
					filenameBytes.length);
			out.write(wholePkg);
			if (meta_buff.length > 0) {
				out.write(meta_buff);
			}

			pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP, 0);

			this.errno = pkgInfo.errno;
			return pkgInfo.errno;
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * get file info decoded from the filename, fetch from the storage if necessary
	 *
	 * @param groupName      the group name
	 * @param remoteFilename the filename
	 * @return FileInfo object for success, return null for fail
	 */
	public FileInfo getFileInfo(String groupName, String remoteFilename) throws IOException, FastDfsException {
		if (remoteFilename.length() < ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH
				+ ProtoCommon.FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
			this.errno = ProtoCommon.ERR_NO_EINVAL;
			return null;
		}

		byte[] buff = base64.decodeAuto(remoteFilename.substring(ProtoCommon.FDFS_FILE_PATH_LEN,
				ProtoCommon.FDFS_FILE_PATH_LEN + ProtoCommon.FDFS_FILENAME_BASE64_LENGTH));

		short file_type;
		long file_size = ProtoCommon.buff2long(buff, 4 * 2);
		if (((file_size & ProtoCommon.APPENDER_FILE_SIZE) != 0)) {
			file_type = FileInfo.FILE_TYPE_APPENDER;
		} else if ((remoteFilename.length() > ProtoCommon.TRUNK_LOGIC_FILENAME_LENGTH) ||
				((remoteFilename.length() > ProtoCommon.NORMAL_LOGIC_FILENAME_LENGTH) &&
						((file_size & ProtoCommon.TRUNK_FILE_MARK_SIZE) == 0))) {
			file_type = FileInfo.FILE_TYPE_SLAVE;
		} else {
			file_type = FileInfo.FILE_TYPE_NORMAL;
		}

		if (file_type == FileInfo.FILE_TYPE_SLAVE ||
				file_type == FileInfo.FILE_TYPE_APPENDER) { //slave file or appender file
			FileInfo fi = this.queryFileInfo(groupName, remoteFilename);
			if (fi == null) {
				return null;
			}

			fi.setFileType(file_type);
			return fi;
		}

		int create_timestamp = ProtoCommon.buff2int(buff, 4);
		if ((file_size >> 63) != 0) {
			file_size &= 0xFFFFFFFFL;  //low 32 bits is file size
		}
		int crc32 = ProtoCommon.buff2int(buff, 4 * 4);

		return new FileInfo(false, file_type, file_size, create_timestamp,
				crc32, ProtoCommon.getIpAddress(buff, 0));
	}

	/**
	 * get file info from storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return FileInfo object for success, return null for fail
	 */
	public FileInfo queryFileInfo(String groupName, String remoteFilename) throws IOException, FastDfsException {
		boolean bNewStorageServer = this.newUpdatableStorageConnection(groupName, remoteFilename);
		Connection connection = this.storageServer.getConnection();
		try {
			byte[] header;
			byte[] groupBytes;
			byte[] filenameBytes;
			byte[] bs;
			int groupLen;
			ProtoCommon.RecvPackageInfo pkgInfo;

			filenameBytes = remoteFilename.getBytes(ClientGlobal.G_CHARSET);
			groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
			bs = groupName.getBytes(ClientGlobal.G_CHARSET);

			Arrays.fill(groupBytes, (byte) 0);
			if (bs.length <= groupBytes.length) {
				groupLen = bs.length;
			} else {
				groupLen = groupBytes.length;
			}
			System.arraycopy(bs, 0, groupBytes, 0, groupLen);

			header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_QUERY_FILE_INFO,
					+groupBytes.length + filenameBytes.length, (byte) 0);
			OutputStream out = connection.getOutputStream();
			byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
			System.arraycopy(header, 0, wholePkg, 0, header.length);
			System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
			System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
			out.write(wholePkg);

			pkgInfo = ProtoCommon.recvPackage(connection.getInputStream(),
					ProtoCommon.STORAGE_PROTO_CMD_RESP,
					3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE +
							ProtoCommon.FDFS_IPADDR_SIZE);

			this.errno = pkgInfo.errno;
			if (pkgInfo.errno != 0) {
				return null;
			}

			long file_size = ProtoCommon.buff2long(pkgInfo.body, 0);
			int create_timestamp = (int) ProtoCommon.buff2long(pkgInfo.body, ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
			int crc32 = (int) ProtoCommon.buff2long(pkgInfo.body, 2 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE);
			String source_ip_addr = (new String(pkgInfo.body, 3 * ProtoCommon.FDFS_PROTO_PKG_LEN_SIZE,
					ProtoCommon.FDFS_IPADDR_SIZE)).trim();
			return new FileInfo(true, FileInfo.FILE_TYPE_NORMAL, file_size,
					create_timestamp, crc32, source_ip_addr);
		} catch (IOException ex) {
			ConnectionUtil.close(connection);
			throw ex;
		} finally {
			releaseConnection(connection, bNewStorageServer);
		}
	}

	/**
	 * check storage socket, if null create a new connection
	 *
	 * @param groupName the group name to upload file to, can be empty
	 * @return true if create a new connection
	 */
	protected boolean newWritableStorageConnection(String groupName) throws IOException, FastDfsException {
		if (this.storageServer != null) {
			return false;
		} else {
			TrackerClient tracker = new TrackerClient();
			this.storageServer = tracker.getStoreStorage(this.trackerServer, groupName);
			if (this.storageServer == null) {
				throw new FastDfsException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
			}
			return true;
		}
	}

	/**
	 * check storage socket, if null create a new connection
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return true if create a new connection
	 */
	protected boolean newReadableStorageConnection(String groupName, String remoteFilename) throws IOException,
			FastDfsException {
		if (this.storageServer != null) {
			return false;
		} else {
			TrackerClient tracker = new TrackerClient();
			this.storageServer = tracker.getFetchStorage(this.trackerServer, groupName, remoteFilename);
			if (this.storageServer == null) {
				throw new FastDfsException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
			}
			return true;
		}
	}

	/**
	 * check storage socket, if null create a new connection
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @return true if create a new connection
	 */
	protected boolean newUpdatableStorageConnection(String groupName, String remoteFilename) throws IOException,
			FastDfsException {
		if (this.storageServer != null) {
			return false;
		} else {
			TrackerClient tracker = new TrackerClient();
			this.storageServer = tracker.getUpdateStorage(this.trackerServer, groupName, remoteFilename);
			if (this.storageServer == null) {
				throw new FastDfsException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
			}
			return true;
		}
	}

	/**
	 * send package to storage server
	 *
	 * @param cmd            which command to send
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 */
	protected void sendPackage(byte cmd, String groupName, String remoteFilename, Connection connection) throws IOException {
		byte[] header;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] bs;
		int groupLen;

		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ClientGlobal.G_CHARSET);
		filenameBytes = remoteFilename.getBytes(ClientGlobal.G_CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(cmd, groupBytes.length + filenameBytes.length, (byte) 0);
		byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
		connection.getOutputStream().write(wholePkg);
	}

	/**
	 * send package to storage server
	 *
	 * @param groupName      the group name of storage server
	 * @param remoteFilename filename on storage server
	 * @param fileOffset     the start offset of the file
	 * @param downloadBytes  download bytes
	 */
	protected void sendDownloadPackage(String groupName, String remoteFilename, long fileOffset,
	                                   long downloadBytes, Connection connection) throws IOException {
		byte[] header;
		byte[] bsOffset;
		byte[] bsDownBytes;
		byte[] groupBytes;
		byte[] filenameBytes;
		byte[] bs;
		int groupLen;

		bsOffset = ProtoCommon.long2buff(fileOffset);
		bsDownBytes = ProtoCommon.long2buff(downloadBytes);
		groupBytes = new byte[ProtoCommon.FDFS_GROUP_NAME_MAX_LEN];
		bs = groupName.getBytes(ClientGlobal.G_CHARSET);
		filenameBytes = remoteFilename.getBytes(ClientGlobal.G_CHARSET);

		Arrays.fill(groupBytes, (byte) 0);
		if (bs.length <= groupBytes.length) {
			groupLen = bs.length;
		} else {
			groupLen = groupBytes.length;
		}
		System.arraycopy(bs, 0, groupBytes, 0, groupLen);

		header = ProtoCommon.packHeader(ProtoCommon.STORAGE_PROTO_CMD_DOWNLOAD_FILE,
				bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length, (byte) 0);
		byte[] wholePkg =
				new byte[header.length + bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length];
		System.arraycopy(header, 0, wholePkg, 0, header.length);
		System.arraycopy(bsOffset, 0, wholePkg, header.length, bsOffset.length);
		System.arraycopy(bsDownBytes, 0, wholePkg, header.length + bsOffset.length, bsDownBytes.length);
		System.arraycopy(groupBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length,
				groupBytes.length);
		System.arraycopy(filenameBytes, 0, wholePkg,
				header.length + bsOffset.length + bsDownBytes.length + groupBytes.length, filenameBytes.length);
		connection.getOutputStream().write(wholePkg);
	}

	public boolean isConnected() {
		return trackerServer != null;
	}

	public boolean isAvailable() {
		return trackerServer != null;
	}

	public void close() {
		trackerServer = null;
	}

	public TrackerServer getTrackerServer() {
		return trackerServer;
	}

	public void setTrackerServer(TrackerServer trackerServer) {
		this.trackerServer = trackerServer;
	}

	public StorageServer getStorageServer() {
		return storageServer;
	}

	public void setStorageServer(StorageServer storageServer) {
		this.storageServer = storageServer;
	}

	/**
	 * Upload file by file buff
	 *
	 * @author Happy Fish / YuQing
	 * @version Version 1.12
	 */
	public static class UploadBuff implements UploadCallback {
		private byte[] fileBuff;
		private int offset;
		private int length;

		/**
		 * constructor
		 *
		 * @param fileBuff the file buff for uploading
		 */
		public UploadBuff(byte[] fileBuff, int offset, int length) {
			super();
			this.fileBuff = fileBuff;
			this.offset = offset;
			this.length = length;
		}

		/**
		 * send file content callback function, be called only once when the file uploaded
		 *
		 * @param out output stream for writing file content
		 * @return 0 success, return none zero(errno) if fail
		 */
		@Override
		public int send(OutputStream out) throws IOException {
			out.write(this.fileBuff, this.offset, this.length);
			return 0;
		}
	}
}
