package top.folen.fastdfs.util;

/**
 * @author sec
 * @version 1.0
 **/
public class StringUtils {

	/**
	 * 判断字符是否为null、空、空格
	 *
	 * @param string 待检查字符串
	 * @return true:空；false：非空
	 */
	public static boolean isBlank(String string) {
		return string == null || string.trim().isEmpty();
	}

	/**
	 * 判断字符串是否不为null、空、空格
	 *
	 * @param string 待检查字符串
	 * @return true:非空；false：空
	 */
	public static boolean isNotBlank(String string) {
		return !isBlank(string);
	}
}
