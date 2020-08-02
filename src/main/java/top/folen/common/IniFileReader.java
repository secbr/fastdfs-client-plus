package top.folen.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * ini file reader / parser
 *
 * @author Happy Fish / YuQing
 * @version Version 1.0
 */
public class IniFileReader {

	private static final Logger logger = LoggerFactory.getLogger(IniFileReader.class);

	private Hashtable paramTable;

	private String confFilename;

	/**
	 * @param confFilename config filename
	 */
	public IniFileReader(String confFilename) {
		this.confFilename = confFilename;
		loadFromFile(confFilename);
	}

	public static ClassLoader classLoader() {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		if (loader == null) {
			loader = ClassLoader.getSystemClassLoader();
		}
		return loader;
	}

	public static InputStream loadFromOsFileSystemOrClasspathAsStream(String filePath) {
		InputStream in = null;
		try {
			// 优先从文件系统路径加载
			if (new File(filePath).exists()) {
				in = new FileInputStream(filePath);
			} else {
				// 从类路径加载
				in = classLoader().getResourceAsStream(filePath);
			}
		} catch (Exception ex) {
			logger.error("读取文件{}异常", filePath, ex);
		}
		return in;
	}

	/**
	 * get the config filename
	 *
	 * @return config filename
	 */
	public String getConfFilename() {
		return this.confFilename;
	}

	/**
	 * get string value from config file
	 *
	 * @param name item name in config file
	 * @return string value
	 */
	public String getStrValue(String name) {
		Object obj = this.paramTable.get(name);
		if (obj == null) {
			return null;
		}

		if (obj instanceof String) {
			return (String) obj;
		}

		return (String) ((ArrayList) obj).get(0);
	}

	/**
	 * get int value from config file
	 *
	 * @param name         item name in config file
	 * @param defaultValue the default value
	 * @return int value
	 */
	public int getIntValue(String name, int defaultValue) {
		String szValue = this.getStrValue(name);
		if (szValue == null) {
			return defaultValue;
		}
		return Integer.parseInt(szValue);
	}

	/**
	 * 获取int值，文件配置改值需大于等0，否则采用默认值
	 *
	 * @param name         item name in config file
	 * @param defaultValue the default value
	 * @return int value
	 */
	public int getPositiveIntValue(String name, int defaultValue) {
		String confValue = this.getStrValue(name);
		if (confValue == null || "".equals(confValue)) {
			return defaultValue;
		}
		int value = Integer.parseInt(confValue);
		return value < 0 ? defaultValue : value;
	}

	/**
	 * get boolean value from config file
	 *
	 * @param name         item name in config file
	 * @param defaultValue the default value
	 * @return boolean value
	 */
	public boolean getBoolValue(String name, boolean defaultValue) {
		String szValue = this.getStrValue(name);
		if (szValue == null) {
			return defaultValue;
		}

		return "yes".equalsIgnoreCase(szValue) || "on".equalsIgnoreCase(szValue) ||
				"true".equalsIgnoreCase(szValue) || "1".equals(szValue);
	}

	/**
	 * get all values from config file
	 *
	 * @param name item name in config file
	 * @return string values (array)
	 */
	public String[] getValues(String name) {
		Object obj;
		String[] values;

		obj = this.paramTable.get(name);
		if (obj == null) {
			return null;
		}

		if (obj instanceof String) {
			values = new String[1];
			values[0] = (String) obj;
			return values;
		}

		Object[] objs = ((ArrayList) obj).toArray();
		values = new String[objs.length];
		System.arraycopy(objs, 0, values, 0, objs.length);
		return values;
	}

	private void loadFromFile(String confFilePath) {
		InputStream in = loadFromOsFileSystemOrClasspathAsStream(confFilePath);
		try {
			readToParamTable(in);
		} catch (Exception ex) {
			logger.error("调用readToParamTable方法", ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception ex) {
				logger.error("关闭读取文件留异常", ex);
			}
		}
	}

	private void readToParamTable(InputStream in) {
		this.paramTable = new Hashtable();
		if (in == null) {
			return;
		}
		String line;
		String[] parts;
		String name;
		String value;
		Object obj;
		ArrayList valueList;
		InputStreamReader inReader = null;
		BufferedReader bufferedReader = null;
		try {
			inReader = new InputStreamReader(in);
			bufferedReader = new BufferedReader(inReader);
			while ((line = bufferedReader.readLine()) != null) {
				line = line.trim();
				if (line.length() == 0 || line.charAt(0) == '#') {
					continue;
				}
				parts = line.split("=", 2);
				if (parts.length != 2) {
					continue;
				}
				name = parts[0].trim();
				value = parts[1].trim();
				obj = this.paramTable.get(name);
				if (obj == null) {
					this.paramTable.put(name, value);
				} else if (obj instanceof String) {
					valueList = new ArrayList();
					valueList.add(obj);
					valueList.add(value);
					this.paramTable.put(name, valueList);
				} else {
					valueList = (ArrayList) obj;
					valueList.add(value);
				}
			}
		} catch (Exception ex) {
			logger.error("读取文件异常", ex);
		} finally {
			try {
				if (bufferedReader != null) {
					bufferedReader.close();
				}
				if (inReader != null) {
					inReader.close();
				}
			} catch (Exception ex) {
				logger.error("关闭文件流异常", ex);
			}
		}
	}

}
