package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.Key;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author JiaKun Xu, 2023-02-16, 10:15
 */
public class FilePathManager {
    private static final Logger LOG = LoggerFactory.getLogger(FilePathManager.class);

    // not blank
    @Nonnull
    private final String suffix;

    // not blank
    @Nonnull
    private final String inUseSuffix;

    public static void validateParameter(@Nonnull Configuration conf) {
        String suffix = conf.getString(Key.SUFFIX);
        String inUseSuffix = conf.getString(Key.IN_USE_SUFFIX);

        /*
         * suffix check
         */
        if (StringUtils.isBlank(suffix)) {
            LOG.warn(String.format("您的 suffix 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_SUFFIX));
            suffix = Constant.DEFAULT_SUFFIX;
        } else {
            suffix = suffix.trim();

            if (suffix.contains("/")) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("请检查参数suffix: [%s], 不可以带有路径分隔符 [/]", suffix)
                );
            }
        }

        conf.set(Key.SUFFIX, suffix);

        /*
         * inUseSuffix check
         */
        if (StringUtils.isBlank(inUseSuffix)) {
            LOG.warn(String.format("您的 inUseSuffix 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_IN_USE_SUFFIX));
            inUseSuffix = Constant.DEFAULT_IN_USE_SUFFIX;
        } else {
            inUseSuffix = inUseSuffix.trim();

            if (inUseSuffix.contains("/")) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("请检查参数inUseSuffix: [%s], 不可以带有路径分隔符 [/]", inUseSuffix)
                );
            }
        }

        conf.set(Key.IN_USE_SUFFIX, inUseSuffix);

        //
        // warn: suffix and inUseSuffix must be different
        //
        if (inUseSuffix.equals(suffix)) {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("suffix: [%s] 与 inUseSuffix: [%s] 不能相同", suffix, inUseSuffix)
            );
        }
    }

    /**
     * ftp helper会在外面关闭
     */
    @Nonnull
    private final BaseFtpHelper ftpHelper;

    @Nonnull
    private final String baseDirectoryPath;

    // not blank
    @Nonnull
    private final String prefixWithUUID;

    private final AtomicInteger fileIndex = new AtomicInteger();

    private String currentFilePath;

    public FilePathManager(@Nonnull Configuration conf, @Nonnull BaseFtpHelper ftpHelper) {
        this.baseDirectoryPath = conf.getString(Key.PATH);
        this.prefixWithUUID = conf.getString(Key.INTERNAL_PREFIX_WITH_UUID);
        this.suffix = conf.getString(Key.SUFFIX);
        this.inUseSuffix = conf.getString(Key.IN_USE_SUFFIX);

        this.ftpHelper = ftpHelper;
    }

    @Nonnull
    private String nextFilePath() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.baseDirectoryPath);
        sb.append("/");  // 不要忘了路径分隔符

        sb.append(this.prefixWithUUID);
        sb.append("-");
        sb.append(this.fileIndex.incrementAndGet());
        sb.append(".").append(this.suffix);
        sb.append(".").append(this.inUseSuffix);

        String tempPath = sb.toString();
        this.currentFilePath = tempPath;  // 不要忘了赋值
        return tempPath;
    }

    @Nonnull
    public String getCurrentFilePath() {
        String tempPath = this.currentFilePath;
        return (tempPath == null) ? this.nextFilePath() : tempPath;
    }

    public void rotate() {
        String tempPath = this.currentFilePath;

        if (tempPath != null) {
            this.currentFilePath = null;

            String destPath = tempPath.substring(0, tempPath.lastIndexOf(this.inUseSuffix) - 1);
            this.ftpHelper.rename(tempPath, destPath);
        }
    }
}
