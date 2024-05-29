package xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.csvreader.CsvWriter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;

/**
 * @author JiaKun Xu, 2023-02-25 15:09
 */
public class CsvWriterImpl extends BaseUnstructuredWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CsvWriterImpl.class);

    private PrintWriter currentPrintWriter;
    private CsvWriter currentCsvWriter;
    private int rollingCount;

    public CsvWriterImpl(@Nonnull Configuration conf) {
        super(conf);

        Assert.assertTrue(Constant.FILE_FORMAT_CSV.equalsIgnoreCase(super.fileFormat));
    }

    @Nonnull
    private CsvWriter getCurrentCsvWriter() throws IOException {
        CsvWriter csvWriter = this.currentCsvWriter;

        if (csvWriter == null) {
            PrintWriter printWriter = this.currentPrintWriter;
            Assert.assertNull(printWriter);

            File file = super.fileManager.getCurrentFile();
            String filePath = file.getAbsolutePath();
            LOG.info(String.format("正在打开文件 [%s] 获取写入流", filePath));

            try {
                // 覆盖，已经检查过文件的重复性
                FileOutputStream fos = new FileOutputStream(file);
                OutputStreamWriter osw = new OutputStreamWriter(fos, super.encoding);
                BufferedWriter bw = new BufferedWriter(osw);
                printWriter = new PrintWriter(bw);

                csvWriter = new CsvWriter(printWriter, super.fieldDelimiter);

                // warn: 下面 2 个都是默认值
                // csvWriter.setTextQualifier('"');
                // csvWriter.setUseTextQualifier(true);

                // warn: 个人还是倾向于使用操作系统自身的换行符
                // csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR_UNIX.charAt(0));
            } catch (IOException ioe) {
                IOException toThrow = new IOException(String.format("打开文件 [%s] 获取写入流时出错", filePath));
                toThrow.addSuppressed(ioe);
                throw toThrow;
            }

            Assert.assertNotNull(printWriter);
            Assert.assertNotNull(csvWriter);
            this.currentPrintWriter = printWriter;
            this.currentCsvWriter = csvWriter;

            // warn: 因为要滚动文件，所以每个滚动的文件都要写入头部信息
            // warn: 头部信息不能作为文件滚动依据
            // warn: 赋值后再写入，因为要finally close
            if (ArrayUtils.isNotEmpty(super.header)) {
                csvWriter.writeRecord(super.header);
            }
        }

        return csvWriter;
    }

    private void rotate() throws IOException {
        CsvWriter csvWriter = this.currentCsvWriter;

        if (csvWriter != null) {
            PrintWriter printWriter = this.currentPrintWriter;
            Assert.assertNotNull(printWriter);

            this.currentPrintWriter = null;
            this.currentCsvWriter = null;
            this.rollingCount = 0;

            csvWriter.close();

            if (printWriter.checkError()) {
                File file = super.fileManager.getCurrentFile();
                throw new IOException(String.format("关闭文件写入流失败 [%s]", file.getAbsolutePath()));
            }

            // 不放到 finally, 因为有写入异常就尽快抛出以结束 datax 任务
            super.fileManager.rotate();
        }
    }

    @Override
    public void writeOneRecord(@Nonnull String[] splitRows) throws IOException {
        CsvWriter csvWriter = getCurrentCsvWriter();

        if (splitRows.length == 0) {
            csvWriter.write(StringUtils.EMPTY);
            csvWriter.endRecord();
        } else {
            csvWriter.writeRecord(splitRows);
        }

        // roll file
        final int rollCount = super.rollCount;

        int rollingCount = this.rollingCount;
        rollingCount += 1;

        if (rollCount > 0 && rollingCount >= rollCount) {
            rollingCount = 0;
            this.rotate();
        }

        this.rollingCount = rollingCount;
    }

    @Override
    public void close() throws IOException {
        rotate();
    }
}
