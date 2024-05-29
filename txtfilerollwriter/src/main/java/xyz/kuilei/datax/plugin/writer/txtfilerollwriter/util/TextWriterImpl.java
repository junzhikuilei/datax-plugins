package xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author JiaKun Xu, 2023-02-14 14:45
 */
public class TextWriterImpl extends BaseUnstructuredWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TextWriterImpl.class);
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    private OutputStream currentOutputStream;
    private long rollingSize;
    private int rollingCount;

    public TextWriterImpl(@Nonnull Configuration conf) {
        super(conf);

        Assert.assertTrue(Constant.FILE_FORMAT_TEXT.equalsIgnoreCase(super.fileFormat));
    }

    @Nonnull
    private OutputStream getCurrentOutputStream() throws IOException {
        OutputStream out = this.currentOutputStream;

        if (out == null) {
            File file = super.fileManager.getCurrentFile();
            String filePath = file.getAbsolutePath();
            LOG.info(String.format("正在打开文件 [%s] 获取写入流", filePath));

            try {
                // 覆盖，已经检查过文件的重复性
                out = new BufferedOutputStream(new FileOutputStream(file));
            } catch (IOException ioe) {
                IOException toThrow = new IOException(String.format("打开文件 [%s] 获取写入流时出错", filePath));
                toThrow.addSuppressed(ioe);
                throw toThrow;
            }

            Assert.assertNotNull(out);
            this.currentOutputStream = out;

            // warn: 因为要滚动文件，所以每个滚动的文件都要写入头部信息
            // warn: 头部信息不能作为文件滚动依据
            // warn: 赋值后再写入，因为要finally close
            if (ArrayUtils.isNotEmpty(super.header)) {
                byte[] line = StringUtils.join(super.header, super.fieldDelimiter).getBytes(super.encoding);
                out.write(line);
                out.write(NEW_LINE);  // remember
            }
        }

        return out;
    }

    private void rotate() throws IOException {
        OutputStream out = this.currentOutputStream;

        if (out != null) {
            this.currentOutputStream = null;
            this.rollingSize = 0;
            this.rollingCount = 0;

            final FileManager fileManager = super.fileManager;

            try {
                out.close();
            } catch (IOException ioe) {
                File file = fileManager.getCurrentFile();
                IOException toThrow = new IOException(String.format("关闭文件写入流失败 [%s]", file.getAbsolutePath()));
                toThrow.addSuppressed(ioe);
                throw toThrow;
            }

            // 不放到 finally, 因为有写入异常就尽快抛出以结束 datax 任务
            fileManager.rotate();
        }
    }

    @Override
    public void writeOneRecord(@Nonnull String[] splitRows) throws IOException {
        // write
        // warn: 之前是把line byte与line separator bytes合并后再写入
        // warn: 想了想：为了效率，还是分开写好一点，这样可以少copy一次内存
        // warn: 前提是使用了buffered output stream
        final OutputStream out = getCurrentOutputStream();
        int written = 0;

        if (splitRows.length != 0) {
            byte[] line = StringUtils.join(splitRows, super.fieldDelimiter).getBytes(super.encoding);
            out.write(line);
            written += line.length;
        }
        out.write(NEW_LINE);
        written += NEW_LINE.length;

        // roll
        boolean rolled = false;

        final long rollSize = super.rollSize;
        final int rollCount = super.rollCount;
        long rollingSize = this.rollingSize;
        int rollingCount = this.rollingCount;

        rollingSize += written;
        rollingCount += 1;

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
