package xyz.kuilei.datax.plugin.writer.ftprollwriter;

/**
 * @author JiaKun Xu, 2023-02-15 15:32
 */
public class Key {
    // ------------------------------------------------
    // 对外开放的参数
    // ------------------------------------------------
    /**
     * base ftp helper
     */
    // must have
    public static final String PROTOCOL = "protocol";

    // must have
    public static final String HOST = "host";

    // not must, default 21(ftp) & 22(sftp)
    public static final String PORT = "port";

    // not must, default 60000
    public static final String TIMEOUT = "timeout";

    // must have
    public static final String USERNAME = "username";

    // must have
    public static final String PASSWORD = "password";


    /**
     * ftp roll writer
     */
    // must have
    public static final String PATH = "path";

    // must have
    public static final String WRITE_MODE = "writeMode";

    // must have
    public static final String PREFIX = "prefix";


    /**
     * datax record reader
     */
    // not must, default "null"
    public static final String NULL_FORMAT = "nullFormat";

    // not must, for writers ' date format
    public static final String DATE_FORMAT = "dateFormat";


    /**
     * base remote writer
     */
    // not must, default text
    public static final String FILE_FORMAT = "fileFormat";

    // not must, default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    // not must, writer headers
    public static final String HEADER = "header";

    // not must, default UTF-8
    public static final String ENCODING = "encoding";

    // not must, default 0
    public static final String ROLL_SIZE = "rollSize";

    // not must, default 0
    public static final String ROLL_COUNT = "rollCount";


    /**
     * file path manager
     */
    // not must, default txt
    public static final String SUFFIX = "suffix";

    // not must, default tmp
    public static final String IN_USE_SUFFIX = "inUseSuffix";


    // ------------------------------------------------
    // 内部参数
    // ------------------------------------------------
    public static final String INTERNAL_PREFIX_WITH_UUID = "internal.prefixWithUUID";
}
