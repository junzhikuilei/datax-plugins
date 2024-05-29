package xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.Key;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.TxtFileRollWriterErrorCode;

import javax.annotation.Nonnull;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * @author JiaKun Xu, 2023-02-14 16:35
 */
public class DataXRecordReader {
    private static final Logger LOG = LoggerFactory.getLogger(DataXRecordReader.class);

    // if is null, set to "null"
    @Nonnull
    private final String nullFormat;

    // if is blank, set to null
    private final String dateFormat;

    public static void validateParameter(@Nonnull Configuration conf) {
        String nullFormat = conf.getString(Key.NULL_FORMAT);
        String dateFormat = conf.getString(Key.DATE_FORMAT);

        /*
         * nullFormat check
         */
        if (null == nullFormat) {
            LOG.warn(String.format("您没有配置nullFormat, 使用默认值 [%s]", Constant.DEFAULT_NULL_FORMAT));
            nullFormat = Constant.DEFAULT_NULL_FORMAT;
        }

        conf.set(Key.NULL_FORMAT, nullFormat);

        /*
         * dateFormat check
         */
        if (StringUtils.isBlank(dateFormat)) {
            dateFormat = null;
        } else {
            dateFormat = dateFormat.trim();

            try {
                new SimpleDateFormat(dateFormat).format(System.currentTimeMillis());
            } catch (RuntimeException re) {
                throw DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的日期格式错误 [%s]", dateFormat),
                        re
                );
            }
        }

        conf.set(Key.DATE_FORMAT, dateFormat);
    }

    // if dateFormat is blank, set dateParse to null
    private final DateFormat dateParse;

    @Nonnull
    private final RecordReceiver recordReceiver;

    public DataXRecordReader(@Nonnull Configuration conf, @Nonnull RecordReceiver recordReceiver) {
        this.nullFormat = conf.getString(Key.NULL_FORMAT);
        this.dateFormat = conf.getString(Key.DATE_FORMAT);
        this.recordReceiver = recordReceiver;

        final String dateFormat = this.dateFormat;
        // warn: 可能不兼容
        if (StringUtils.isBlank(dateFormat)) {
            this.dateParse = null;
        } else {
            this.dateParse = new SimpleDateFormat(dateFormat);
        }
    }

    /**
     * @return null -> finished
     *         not null -> still has records to read
     */
    public String[] readOneRecord() {
        Record rec = this.recordReceiver.getFromReader();
        if (rec == null) {
            return null;
        }

        int recLen = rec.getColumnNumber();
        if (0 == recLen) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        final String nullFormat = this.nullFormat;
        final DateFormat dateParse = this.dateParse;

        String[] splitRows = new String[recLen];

        for (int i = 0; i < recLen; i++) {
            Column col = rec.getColumn(i);
            if (null != col.getRawData()) {
                if (dateParse != null && col instanceof DateColumn) {
                    splitRows[i] = dateParse.format(col.asDate());
                } else {
                    splitRows[i] = col.asString();
                }
            } else {
                splitRows[i] = nullFormat;
            }
        }

        return splitRows;
    }
}
