package xyz.kuilei.datax.plugin.writer.ftprollwriter.unuse;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.Key;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.BaseFtpHelper;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.Constant;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.FilePathManager;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.SelfArrayUtil;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author JiaKun Xu, 2023-02-16 10:33
 */
@Deprecated
public class FtpStorageWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FtpStorageWriter.class);

    // single character
    private final char fieldDelimiter;

    // if is empty, set to null
    private final String[] header;

    // not blank
    @Nonnull
    private final String encoding;

    // >= 0
    private final long rollSize;

    // >= 0
    private final int rollCount;

    public static void validateParameter(@Nonnull Configuration conf) {
        /*
         * header check
         */
        // do nothing

        /*
         * encoding check
         */
        String encoding = conf.getString(Key.ENCODING);

        if (StringUtils.isBlank(encoding)) {
            LOG.warn(String.format("您的 encoding 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_ENCODING));
            conf.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
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

            conf.set(Key.ENCODING, encoding);
        }

        /*
         * rollSize check
         */
        Long rollSize = conf.getLong(Key.ROLL_SIZE);

        if (null == rollSize) {
            LOG.warn(String.format("您没有配置 rollSize, 使用默认值 [%s]", Constant.DEFAULT_ROLL_SIZE));
            conf.set(Key.ROLL_SIZE, Constant.DEFAULT_ROLL_SIZE);
        } else {
            if (rollSize < 0L) {
                conf.set(Key.ROLL_SIZE, 0L);
            }
        }

        /*
         * rollCount check
         */
        Integer rollCount = conf.getInt(Key.ROLL_COUNT);

        if (null == rollCount) {
            LOG.warn(String.format("您没有配置 rollCount, 使用默认值 [%s]", Constant.DEFAULT_ROLL_COUNT));
            conf.set(Key.ROLL_COUNT, Constant.DEFAULT_ROLL_COUNT);
        } else {
            if (rollCount < 0) {
                conf.set(Key.ROLL_COUNT, 0);
            }
        }
    }

    /**
     * ftp helper会在外面关闭
     */
    @Nonnull
    private final BaseFtpHelper ftpHelper;

    @Nonnull
    private final FilePathManager filePathManager;

    private OutputStream currentOutputStream;

    private long rollingSize;

    private int rollingCount;

    public FtpStorageWriter(@Nonnull Configuration conf, @Nonnull BaseFtpHelper ftpHelper) {
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

    @Nonnull
    private OutputStream getCurrentOutputStream() throws IOException {
        OutputStream outputStream = this.currentOutputStream;

        if (outputStream == null) {
            String filePath = this.filePathManager.getCurrentFilePath();
            LOG.info(String.format("正在打开文件 [%s] 获取写入流", filePath));

            outputStream = this.ftpHelper.getOutputStream(filePath);
            this.currentOutputStream = outputStream;

            if (ArrayUtils.isNotEmpty(this.header)) {
                byte[] lineBytes = StringUtils.join(this.header, this.fieldDelimiter).getBytes(this.encoding);
                byte[] bytesToWrite = combineLineSeparatorBytes(lineBytes);
                outputStream.write(bytesToWrite);
            }
        }

        return outputStream;
    }

    private void rotate() throws IOException {
        OutputStream outputStream = this.currentOutputStream;
        FilePathManager filePathManager = this.filePathManager;

        if (outputStream != null) {
            this.currentOutputStream = null;
            this.rollingSize = 0;
            this.rollingCount = 0;

            String filePath = filePathManager.getCurrentFilePath();

            try {
                outputStream.close();
            } catch (IOException ioe) {
                IOException toThrow = new IOException(String.format("关闭文件写入流失败 [%s]", filePath));
                toThrow.addSuppressed(ioe);
                throw toThrow;
            }

            // 1. 不放到 finally, 为的是有写入异常就尽快抛出以结束 datax 任务
            // 2. 这里最好来个complete pending commands，但必须在close stream之后。
            //    file path manager里面已经有complete pending commands了，wow！
            filePathManager.rotate();
        }
    }

    @Nonnull
    private static byte[] combineLineSeparatorBytes(@Nonnull byte[] abuf) {
        final byte[] bbuf = NEW_LINE;

        int alen = abuf.length;
        int blen = bbuf.length;

        byte[] bytes = new byte[alen + blen];
        System.arraycopy(abuf, 0, bytes, 0, alen);
        System.arraycopy(bbuf, 0, bytes, alen, blen);
        return bytes;
    }

    // warn: 远程传输统一使用unix标准
    private static final byte[] NEW_LINE = "\n".getBytes(StandardCharsets.UTF_8);

    public void writeOneLine(@Nonnull String line) throws IOException {
        // write bytes with line separator
        OutputStream outputStream = this.getCurrentOutputStream();
        byte[] bytes = null;
        if (line.length() == 0) {
            bytes = NEW_LINE;
        } else {
            bytes = combineLineSeparatorBytes(line.getBytes(encoding));
        }
        outputStream.write(bytes);

        int writtenSize = bytes.length;

        // roll file
        final long rollSize = this.rollSize;
        final int rollCount = this.rollCount;
        long rollingSize = this.rollingSize;
        int rollingCount = this.rollingCount;

        rollingSize += writtenSize;
        rollingCount += 1;

        boolean rolled = false;
        if (rollSize > 0 && rollingSize >= rollSize) {
            rollingSize = 0;
            rollingCount = 0;
            this.rotate();
            rolled = true;
        }
        if (!rolled) {
            if (rollCount > 0 && rollingCount >= rollCount) {
                rollingSize = 0;
                rollingCount = 0;
                this.rotate();
                // rolled = true;
            }
        }

        this.rollingSize = rollingSize;
        this.rollingCount = rollingCount;
    }

    @Override
    public void close() throws IOException {
        this.rotate();
    }
}
