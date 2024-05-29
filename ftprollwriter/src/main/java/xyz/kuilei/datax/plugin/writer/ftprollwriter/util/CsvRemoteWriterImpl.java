package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

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
 * @author JiaKun Xu, 2023-02-27 20:09
 */
public class CsvRemoteWriterImpl extends BaseRemoteWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CsvRemoteWriterImpl.class);

    private PrintWriter currentPrintWriter;
    private CsvWriter currentCsvWriter;
    private int rollingCount;

    public CsvRemoteWriterImpl(@Nonnull Configuration conf, @Nonnull BaseFtpHelper ftpHelper) {
        super(conf, ftpHelper);

        Assert.assertTrue(Constant.FILE_FORMAT_CSV.equalsIgnoreCase(super.fileFormat));
    }

    @Nonnull
    private CsvWriter getCurrentCsvWriter() throws IOException {
        CsvWriter csvWriter = this.currentCsvWriter;

        if (csvWriter == null) {
            PrintWriter printWriter = this.currentPrintWriter;
            Assert.assertNull(printWriter);

            String filePath = super.filePathManager.getCurrentFilePath();
            LOG.info(String.format("正在打开文件 [%s] 获取写入流", filePath));

            try {
                OutputStream os = super.ftpHelper.getOutputStream(filePath);
                OutputStreamWriter osw = new OutputStreamWriter(os, super.encoding);
                BufferedWriter bw = new BufferedWriter(osw);

                printWriter = new PrintWriter(bw);
                csvWriter = new CsvWriter(printWriter, super.fieldDelimiter);

                csvWriter.setRecordDelimiter(LINE_SEPARATOR_CHAR);
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

            FilePathManager filePathManager = super.filePathManager;

            if (printWriter.checkError()) {
                String filePath = filePathManager.getCurrentFilePath();
                throw new IOException(String.format("关闭文件写入流失败 [%s]", filePath));
            }

            // 不放到 finally, 因为有写入异常就尽快抛出以结束 datax 任务
            filePathManager.rotate();
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
