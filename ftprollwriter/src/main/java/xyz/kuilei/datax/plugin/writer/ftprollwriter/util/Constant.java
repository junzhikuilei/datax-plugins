package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

/**
 * @author JiaKun Xu, 2023-02-15 15:33
 */
public class Constant {
    /**
     * base ftp helper
     */
    public static final String PROTOCOL_FTP = "ftp";

    public static final String PROTOCOL_SFTP = "sftp";

    public static final int PORT_FTP = 21;

    public static final int PORT_SFTP = 22;

    public static final int DEFAULT_TIMEOUT = 10000;

    /**
     * datax record reader
     */
    public static final String DEFAULT_NULL_FORMAT = "null";

    /**
     * base remote writer
     */
    public static final String FILE_FORMAT_TEXT = "text";

    public static final String FILE_FORMAT_CSV = "csv";

    public static final char DEFAULT_FIELD_DELIMITER = ',';

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final long DEFAULT_ROLL_SIZE = 0L;

    public static final int DEFAULT_ROLL_COUNT = 0;

    /**
     * file path manager
     */
    public static final String DEFAULT_SUFFIX = "txt";

    public static final String DEFAULT_IN_USE_SUFFIX = "tmp";
}
