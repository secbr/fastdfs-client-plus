package top.folen.fastdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.folen.common.FastDfsException;
import top.folen.common.NameValuePair;

import java.io.IOException;

/**
 * Storage client for 1 field file id: combined group name and filename
 * Note: the instance of this class is NOT thread safe !!!
 * if not necessary, do NOT set storage server instance
 *
 * @author Happy Fish / YuQing
 * @version Version 1.27
 */
public class StorageClient1 extends StorageClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient1.class);
	
	public static final String SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR = "/";

	/**
	 * constructor
	 */
	public StorageClient1() {
		super();
	}

	/**
	 * constructor with trackerServer
	 *
	 * @param trackerServer the tracker server, can be null
	 */
	public StorageClient1(TrackerServer trackerServer) {
		super(trackerServer);
	}

	/**
	 * constructor with trackerServer and storageServer
	 * NOTE: if not necessary, do NOT set storage server instance
	 *
	 * @param trackerServer the tracker server, can be null
	 * @param storageServer the storage server, can be null
	 */
	public StorageClient1(TrackerServer trackerServer, StorageServer storageServer) {
		super(trackerServer, storageServer);
	}

	public static byte splitFileId(String fileId, String[] results) {
		int pos = fileId.indexOf(SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR);
		if ((pos <= 0) || (pos == fileId.length() - 1)) {
			return ProtoCommon.ERR_NO_EINVAL;
		}

		results[0] = fileId.substring(0, pos); //group name
		results[1] = fileId.substring(pos + 1); //file name
		return 0;
	}

	/**
	 * upload file to storage server (by file name)
	 *
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String localFilename, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadFile(localFilename, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file name)
	 *
	 * @param groupName     the group name to upload file to, can be empty
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String groupName, String localFilename, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadFile(groupName, localFilename, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(byte[] fileBuff, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadFile(fileBuff, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file buff)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String groupName, byte[] fileBuff, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadFile(groupName, fileBuff, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by callback)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileSize    the file size
	 * @param callback    the write data callback object
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String groupName, long fileSize,
	                          UploadCallback callback, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadFile(groupName, fileSize, callback, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload appender file to storage server (by file name)
	 *
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadAppenderFile1(String localFilename, String fileExtName,
	                                  NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadAppenderFile(localFilename, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload appender file to storage server (by file name)
	 *
	 * @param group_name    the group name to upload file to, can be empty
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadAppenderFile1(String group_name, String localFilename, String fileExtName,
	                                  NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadAppenderFile(group_name, localFilename, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadAppenderFile1(byte[] fileBuff, String fileExtName,
	                                  NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadAppenderFile(fileBuff, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload appender file to storage server (by file buff)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileBuff    file content/buff
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadAppenderFile1(String groupName, byte[] fileBuff, String fileExtName,
	                                  NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadAppenderFile(groupName, fileBuff, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload appender file to storage server (by callback)
	 *
	 * @param groupName   the group name to upload file to, can be empty
	 * @param fileSize    the file size
	 * @param callback    the write data callback object
	 * @param fileExtName file ext name, do not include dot(.)
	 * @param metaList    meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadAppenderFile1(String groupName, long fileSize,
	                                  UploadCallback callback, String fileExtName,
	                                  NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = this.uploadAppenderFile(groupName, fileSize, callback, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file name, slave file mode)
	 *
	 * @param masterFileId  the master file id to generate the slave file
	 * @param prefixName    the prefix name to generate the slave file
	 * @param localFilename local filename to upload
	 * @param fileExtName   file ext name, do not include dot(.), null to extract ext name from the local filename
	 * @param metaList      meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String masterFileId, String prefixName,
	                          String localFilename, String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(masterFileId, parts);
		if (this.errno != 0) {
			return null;
		}

		parts = this.uploadFile(parts[0], parts[1], prefixName,
				localFilename, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file buff, slave file mode)
	 *
	 * @param masterFileId the master file id to generate the slave file
	 * @param prefixName   the prefix name to generate the slave file
	 * @param fileBuff     file content/buff
	 * @param fileExtName  file ext name, do not include dot(.)
	 * @param metaList     meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String masterFileId, String prefixName,
	                          byte[] fileBuff, String fileExtName, NameValuePair[] metaList) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(masterFileId, parts);
		if (this.errno != 0) {
			return null;
		}

		parts = this.uploadFile(parts[0], parts[1], prefixName, fileBuff, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by file buff, slave file mode)
	 *
	 * @param masterFileId the master file id to generate the slave file
	 * @param prefixName   the prefix name to generate the slave file
	 * @param fileBuff     file content/buff
	 * @param fileExtName  file ext name, do not include dot(.)
	 * @param metaList     meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String masterFileId, String prefixName,
	                          byte[] fileBuff, int offset, int length, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(masterFileId, parts);
		if (this.errno != 0) {
			return null;
		}

		parts = this.uploadFile(parts[0], parts[1], prefixName, fileBuff,
				offset, length, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * upload file to storage server (by callback)
	 *
	 * @param masterFileId the master file id to generate the slave file
	 * @param prefixName   the prefix name to generate the slave file
	 * @param fileSize     the file size
	 * @param callback     the write data callback object
	 * @param fileExtName  file ext name, do not include dot(.)
	 * @param metaList     meta info array
	 * @return file id(including group name and filename) if success, <br>
	 * return null if fail
	 */
	public String uploadFile1(String masterFileId, String prefixName, long fileSize,
	                          UploadCallback callback, String fileExtName,
	                          NameValuePair[] metaList) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(masterFileId, parts);
		if (this.errno != 0) {
			return null;
		}

		parts = this.uploadFile(parts[0], parts[1], prefixName, fileSize, callback, fileExtName, metaList);
		if (parts != null) {
			return parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + parts[1];
		} else {
			return null;
		}
	}

	/**
	 * append file to storage server (by file name)
	 *
	 * @param appenderFileId the appender file id
	 * @param localFilename  local filename to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile1(String appenderFileId, String localFilename) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.appendFile(parts[0], parts[1], localFilename);
	}

	/**
	 * append file to storage server (by file buff)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileBuff       file content/buff
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile1(String appenderFileId, byte[] fileBuff) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.appendFile(parts[0], parts[1], fileBuff);
	}

	/**
	 * append file to storage server (by file buff)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileBuff       file content/buffer
	 * @param offset         start offset of the buffer
	 * @param length         the length of the buffer to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile1(String appenderFileId, byte[] fileBuff, int offset, int length) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.appendFile(parts[0], parts[1], fileBuff, offset, length);
	}

	/**
	 * append file to storage server (by callback)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileSize       the file size
	 * @param callback       the write data callback object
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int appendFile1(String appenderFileId, long fileSize, UploadCallback callback) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.appendFile(parts[0], parts[1], fileSize, callback);
	}

	/**
	 * modify appender file to storage server (by file name)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileOffset     the offset of appender file
	 * @param localFilename  local filename to append
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile1(String appenderFileId,
	                       long fileOffset, String localFilename) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.modifyFile(parts[0], parts[1], fileOffset, localFilename);
	}

	/**
	 * modify appender file to storage server (by file buff)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileOffset     the offset of appender file
	 * @param fileBuff       file content/buff
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile1(String appenderFileId,
	                       long fileOffset, byte[] fileBuff) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.modifyFile(parts[0], parts[1], fileOffset, fileBuff);
	}

	/**
	 * modify appender file to storage server (by file buff)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileOffset     the offset of appender file
	 * @param fileBuff       file content/buff
	 * @param bufferOffset   start offset of the buff
	 * @param bufferLength   the length of buff to modify
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile1(String appenderFileId,
	                       long fileOffset, byte[] fileBuff, int bufferOffset, int bufferLength) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.modifyFile(parts[0], parts[1], fileOffset,
				fileBuff, bufferOffset, bufferLength);
	}

	/**
	 * modify appender file to storage server (by callback)
	 *
	 * @param appenderFileId the appender file id
	 * @param fileOffset     the offset of appender file
	 * @param modifySize     the modify size
	 * @param callback       the write data callback object
	 * @return 0 for success, != 0 for error (error no)
	 */
	public int modifyFile1(String appenderFileId,
	                       long fileOffset, long modifySize, UploadCallback callback) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.modifyFile(parts[0], parts[1], fileOffset, modifySize, callback);
	}

	/**
	 * regenerate filename for appender file
	 *
	 * @param appenderFileId the appender file id
	 * @return the regenerated file id, return null if fail
	 */
	public String regenerateAppenderFilename1(String appenderFileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return null;
		}

		String[] new_parts = this.regenerateAppenderFilename(parts[0], parts[1]);
		if (new_parts != null) {
			return new_parts[0] + SPLIT_GROUP_NAME_AND_FILENAME_SEPARATOR + new_parts[1];
		} else {
			return null;
		}
	}

	/**
	 * delete file from storage server
	 *
	 * @param fileId the file id(including group name and filename)
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int deleteFile1(String fileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.deleteFile(parts[0], parts[1]);
	}

	/**
	 * truncate appender file to size 0 from storage server
	 *
	 * @param appenderFileId the appender file id
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int truncateFile1(String appenderFileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.truncateFile(parts[0], parts[1]);
	}

	/**
	 * truncate appender file from storage server
	 *
	 * @param appenderFileId    the appender file id
	 * @param truncatedFileSize truncated file size
	 * @return 0 for success, none zero for fail (error code)
	 */
	public int truncateFile1(String appenderFileId, long truncatedFileSize) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(appenderFileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.truncateFile(parts[0], parts[1], truncatedFileSize);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId the file id(including group name and filename)
	 * @return file content/buffer, return null if fail
	 */
	public byte[] downloadFile1(String fileId) throws IOException, FastDfsException {
		return this.downloadFile1(fileId, 0, 0);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId        the file id(including group name and filename)
	 * @param fileOffset    the start offset of the file
	 * @param downloadBytes download bytes, 0 for remain bytes from offset
	 * @return file content/buff, return null if fail
	 */
	public byte[] downloadFile1(String fileId, long fileOffset, long downloadBytes) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return null;
		}

		return this.downloadFile(parts[0], parts[1], fileOffset, downloadBytes);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId        the file id(including group name and filename)
	 * @param localFilename the filename on local
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile1(String fileId, String localFilename) throws IOException, FastDfsException {
		final long file_offset = 0;
		final long download_bytes = 0;

		return this.downloadFile1(fileId, file_offset, download_bytes, localFilename);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId        the file id(including group name and filename)
	 * @param fileOffset    the start offset of the file
	 * @param downloadBytes download bytes, 0 for remain bytes from offset
	 * @param localFilename the filename on local
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile1(String fileId, long fileOffset, long downloadBytes, String localFilename) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.downloadFile(parts[0], parts[1], fileOffset, downloadBytes, localFilename);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId   the file id(including group name and filename)
	 * @param callback the callback object, will call callback.recv() when data arrive
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile1(String fileId, DownloadCallback callback) throws IOException, FastDfsException {
		return this.downloadFile1(fileId, 0, 0, callback);
	}

	/**
	 * download file from storage server
	 *
	 * @param fileId        the file id(including group name and filename)
	 * @param fileOffset    the start offset of the file
	 * @param downloadBytes download bytes, 0 for remain bytes from offset
	 * @param callback      the callback object, will call callback.recv() when data arrive
	 * @return 0 success, return none zero errno if fail
	 */
	public int downloadFile1(String fileId, long fileOffset, long downloadBytes, DownloadCallback callback) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.downloadFile(parts[0], parts[1], fileOffset, downloadBytes, callback);
	}

	/**
	 * get all metadata items from storage server
	 *
	 * @param fileId the file id(including group name and filename)
	 * @return meta info array, return null if fail
	 */
	public NameValuePair[] getMetadata1(String fileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return null;
		}

		return this.getMetadata(parts[0], parts[1]);
	}

	/**
	 * set metadata items to storage server
	 *
	 * @param fileId   the file id(including group name and filename)
	 * @param metaList meta item array
	 * @param opFlag   flag, can be one of following values: <br>
	 *                 <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_OVERWRITE: overwrite all old
	 *                 metadata items</li></ul>
	 *                 <ul><li> ProtoCommon.STORAGE_SET_METADATA_FLAG_MERGE: merge, insert when
	 *                 the metadata item not exist, otherwise update it</li></ul>
	 * @return 0 for success, !=0 fail (error code)
	 */
	public int setMetadata1(String fileId, NameValuePair[] metaList, byte opFlag) throws IOException,
			FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return this.errno;
		}

		return this.setMetadata(parts[0], parts[1], metaList, opFlag);
	}

	/**
	 * get file info from storage server
	 *
	 * @param fileId the file id(including group name and filename)
	 * @return FileInfo object for success, return null for fail
	 */
	public FileInfo queryFileInfo1(String fileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return null;
		}

		return this.queryFileInfo(parts[0], parts[1]);
	}

	/**
	 * get file info decoded from filename
	 *
	 * @param fileId the file id(including group name and filename)
	 * @return FileInfo object for success, return null for fail
	 */
	public FileInfo getFileInfo1(String fileId) throws IOException, FastDfsException {
		String[] parts = new String[2];
		this.errno = splitFileId(fileId, parts);
		if (this.errno != 0) {
			return null;
		}

		return this.getFileInfo(parts[0], parts[1]);
	}
}
