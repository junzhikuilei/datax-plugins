package xyz.kuilei.datax.plugin.writer.txtfilerollwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util.FileManager;

/**
 * TODO NOTE:
 * 1. 在目前的使用场景中，不需要 compress 文本数据
 * 2. roll size, roll count 可以满足文件滚动需求
 * 3. roll interval 不应实现，因为 datax 不适合做流式处理
 * 4. 如果 path 在 validate parameter 时就可以确定有效性，那么无需添加内部参数 absolute path
 *
 * @author JiaKun Xu, 2023-02-14 10:44
 */
public class Key {
    // ------------------------------------------------
    // 对外开放的参数
    // ------------------------------------------------
    /**
     * txt file roll writer
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
     * base unstructured writer
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
     * file manager
     */
    // not must, default txt
    public static final String SUFFIX = "suffix";

    // not must, default tmp
    public static final String IN_USE_SUFFIX = "inUseSuffix";


    // ------------------------------------------------
    // 内部参数
    // ------------------------------------------------
    /**
     * @see TxtFileRollWriter.Job#split(int)
     * @see TxtFileRollWriter.Task#startWrite(RecordReceiver)
     * @see FileManager#prefixWithUUID
     */
    public static final String INTERNAL_PREFIX_WITH_UUID = "internal.prefixWithUUID";
}
