package top.folen.fastdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Upload file by stream
 *
 * @author zhouzezhong & Happy Fish / YuQing
 * @version Version 1.11
 */
public class UploadStream implements UploadCallback {

	private static final Logger LOGGER = LoggerFactory.getLogger(UploadStream.class);

	/**
	 * input stream for reading
	 */
	private final InputStream inputStream;

	/**
	 * size of the uploaded file
	 */
	private final long fileSize;

	/**
	 * constructor
	 *
	 * @param inputStream input stream for uploading
	 * @param fileSize    size of uploaded file
	 */
	public UploadStream(InputStream inputStream, long fileSize) {
		super();
		this.inputStream = inputStream;
		this.fileSize = fileSize;
	}

	/**
	 * send file content callback function, be called only once when the file uploaded
	 *
	 * @param out output stream for writing file content
	 * @return 0 success, return none zero(errno) if fail
	 */
	@Override
	public int send(OutputStream out) throws IOException {
		long remainBytes = fileSize;
		byte[] buff = new byte[256 * 1024];
		int bytes;
		while (remainBytes > 0) {
			try {
				if ((bytes = inputStream.read(buff, 0, remainBytes > buff.length ? buff.length : (int) remainBytes)) < 0) {
					return -1;
				}
			} catch (IOException ex) {
				LOGGER.error("发送异常", ex);
				return -1;
			}
			out.write(buff, 0, bytes);
			remainBytes -= bytes;
		}

		return 0;
	}
}
