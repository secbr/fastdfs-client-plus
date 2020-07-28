package top.folen.fastdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Download file by stream (download callback class)
 *
 * @author zhouzezhong & Happy Fish / YuQing
 * @version Version 1.11
 */
public class DownloadStream implements DownloadCallback {

	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadStream.class);

	private OutputStream out;

	private long currentBytes = 0;

	public DownloadStream(OutputStream out) {
		super();
		this.out = out;
	}

	/**
	 * recv file content callback function, may be called more than once when the file downloaded
	 *
	 * @param fileSize file size
	 * @param data     data buff
	 * @param bytes    data bytes
	 * @return 0 success, return none zero(errno) if fail
	 */
	@Override
	public int recv(long fileSize, byte[] data, int bytes) {
		try {
			out.write(data, 0, bytes);
		} catch (IOException ex) {
			LOGGER.error("写文件异常", ex);
			return -1;
		}
		currentBytes += bytes;
		if (this.currentBytes == fileSize) {
			this.currentBytes = 0;
		}
		return 0;
	}
}
