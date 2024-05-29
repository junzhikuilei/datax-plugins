package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.Key;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author JiaKun Xu, 2023-02-27 20:09
 */
public abstract class BaseRemoteWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BaseRemoteWriter.class);

    // 远程传输统一使用unix标准
    protected static final String LINE_SEPARATOR = "\n";

    protected static final char LINE_SEPARATOR_CHAR = '\n';

    protected static final byte[] NEW_LINE = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);

    // not blank
    @Nonnull
    protected final String fileFormat;

    // single character
    protected final char fieldDelimiter;

    // if is empty, set to null
    protected final String[] header;

    // not blank
    @Nonnull
    protected final String encoding;

    // >= 0
    protected final long rollSize;

    // >= 0
    protected final int rollCount;

    public static void validateParameter(@Nonnull Configuration conf) {
        String fileFormat = conf.getString(Key.FILE_FORMAT);
        String fieldDelimiter = conf.getString(Key.FIELD_DELIMITER);
        List<String> header = conf.getList(Key.HEADER, String.class);
        String encoding = conf.getString(Key.ENCODING);
        Long rollSize = conf.getLong(Key.ROLL_SIZE);
        Integer rollCount = conf.getInt(Key.ROLL_COUNT);

        /*
         * fileFormat check
         */
        if (StringUtils.isBlank(fileFormat)) {
            LOG.warn(String.format("您没有配置文件格式fileFormat, 使用默认值 [%s]", Constant.FILE_FORMAT_TEXT));
            fileFormat = Constant.FILE_FORMAT_TEXT;
        } else {
            fileFormat = fileFormat.trim();

            if (Constant.FILE_FORMAT_TEXT.equalsIgnoreCase(fileFormat)) {
                fileFormat = Constant.FILE_FORMAT_TEXT;
            } else if (Constant.FILE_FORMAT_CSV.equalsIgnoreCase(fileFormat)) {
                fileFormat = Constant.FILE_FORMAT_CSV;
            } else {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的fileFormat: [%s]错误, 支持csv, text两种", fileFormat)
                );
            }
        }

        conf.set(Key.FILE_FORMAT, fileFormat);

        /*
         * fieldDelimiter check
         */
        // warn: if have, length must be one
        if (null == fieldDelimiter) {
            LOG.warn(String.format("您没有配置列分隔符, 使用默认值 [%s]", Constant.DEFAULT_FIELD_DELIMITER));
            conf.set(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        } else {
            if (1 != fieldDelimiter.length()) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持单字符切分, 您配置的切分为 [%s]", fieldDelimiter)
                );
            }
        }

        /*
         * header check
         */
        // do nothing

        /*
         * encoding check
         */
        if (StringUtils.isBlank(encoding)) {
            LOG.warn(String.format("您的 encoding 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_ENCODING));
            encoding = Constant.DEFAULT_ENCODING;
        } else {
            encoding = encoding.trim();

            try {
                Charset.forName(encoding);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式 [%s]", encoding),
                        e
                );
            }
        }

        conf.set(Key.ENCODING, encoding);

        /*
         * rollSize check
         */
        if (null == rollSize) {
            LOG.warn(String.format("您没有配置 rollSize, 使用默认值 [%d]", Constant.DEFAULT_ROLL_SIZE));
            rollSize = Constant.DEFAULT_ROLL_SIZE;
        } else {
            if (rollSize < 0L) {
                rollSize = 0L;
            }
        }

        if (Constant.FILE_FORMAT_CSV.equalsIgnoreCase(fileFormat)) {
            LOG.warn("当fileFormat: [csv]时，不支持按写入字节数 rollSize 滚动文件");
            rollSize = 0L;
        }

        conf.set(Key.ROLL_SIZE, rollSize);

        /*
         * rollCount check
         */
        if (null == rollCount) {
            LOG.warn(String.format("您没有配置 rollCount, 使用默认值 [%s]", Constant.DEFAULT_ROLL_COUNT));
            rollCount = Constant.DEFAULT_ROLL_COUNT;
        } else {
            if (rollCount < 0) {
                rollCount = 0;
            }
        }

        conf.set(Key.ROLL_COUNT, rollCount);
    }

    @Nonnull
    protected final BaseFtpHelper ftpHelper;

    @Nonnull
    protected final FilePathManager filePathManager;

    protected BaseRemoteWriter(@Nonnull Configuration conf, @Nonnull BaseFtpHelper ftpHelper) {
        this.fileFormat = conf.getString(Key.FILE_FORMAT);
        this.fieldDelimiter = conf.getChar(Key.FIELD_DELIMITER);
        this.encoding = conf.getString(Key.ENCODING);
        this.rollSize = conf.getLong(Key.ROLL_SIZE);
        this.rollCount = conf.getInt(Key.ROLL_COUNT);

        this.ftpHelper = ftpHelper;
        this.filePathManager = new FilePathManager(conf, ftpHelper);

        List<String> headerList = conf.getList(Key.HEADER, String.class);
        final int headNumb = (headerList == null) ? 0 : headerList.size();
        if (headNumb != 0) {
            this.header = SelfArrayUtil.toStringArray(headerList.toArray());
        } else {
            this.header = null;
        }
    }

    public abstract void writeOneRecord(@Nonnull String[] splitRows) throws IOException;

    @Nonnull
    public static BaseRemoteWriter getImpl(@Nonnull Configuration conf, @Nonnull BaseFtpHelper ftpHelper) {
        String fileFormat = conf.getString(Key.FILE_FORMAT);

        if (Constant.FILE_FORMAT_TEXT.equalsIgnoreCase(fileFormat)) {
            return new TextRemoteWriterImpl(conf, ftpHelper);
        } else if (Constant.FILE_FORMAT_CSV.equalsIgnoreCase(fileFormat)) {
            return new CsvRemoteWriterImpl(conf, ftpHelper);
        } else {
            throw new IllegalStateException("should not happen");
        }
    }
}
