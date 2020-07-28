package top.folen.fastdfs;

import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * C struct body decoder
 *
 * @author zzs
 * @version Version 1.1.0
 */
public abstract class BaseStruct {

	/**
	 * set fields
	 *
	 * @param bs     byte array
	 * @param offset start offset
	 */
	public abstract void setFields(byte[] bs, int offset);

	protected String stringValue(byte[] bs, int offset, FieldInfo filedInfo) {
		try {
			return (new String(bs, offset + filedInfo.offset, filedInfo.size, ClientGlobal.G_CHARSET)).trim();
		} catch (UnsupportedEncodingException ex) {
			ex.printStackTrace();
			return null;
		}
	}

	protected long longValue(byte[] bs, int offset, FieldInfo filedInfo) {
		return ProtoCommon.buff2long(bs, offset + filedInfo.offset);
	}

	protected int intValue(byte[] bs, int offset, FieldInfo filedInfo) {
		return (int) ProtoCommon.buff2long(bs, offset + filedInfo.offset);
	}

	protected int int32Value(byte[] bs, int offset, FieldInfo filedInfo) {
		return ProtoCommon.buff2int(bs, offset + filedInfo.offset);
	}

	protected byte byteValue(byte[] bs, int offset, FieldInfo filedInfo) {
		return bs[offset + filedInfo.offset];
	}

	protected boolean booleanValue(byte[] bs, int offset, FieldInfo filedInfo) {
		return bs[offset + filedInfo.offset] != 0;
	}

	protected Date dateValue(byte[] bs, int offset, FieldInfo filedInfo) {
		return new Date(ProtoCommon.buff2long(bs, offset + filedInfo.offset) * 1000);
	}

	protected static class FieldInfo {
		protected String name;
		protected int offset;
		protected int size;

		public FieldInfo(String name, int offset, int size) {
			this.name = name;
			this.offset = offset;
			this.size = size;
		}
	}
}