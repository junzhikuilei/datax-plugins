package xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.Key;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.TxtFileRollWriterErrorCode;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author JiaKun Xu, 2023-02-14, 15:59
 */
public class FileManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    // not blank
    @Nonnull
    private final String suffix;

    // not blank
    @Nonnull
    private final String inUseSuffix;

    public static void validateParameter(Configuration conf) {
        /*
         * suffix check
         */
        String suffix = conf.getString(Key.SUFFIX);

        if (StringUtils.isBlank(suffix)) {
            LOG.warn(String.format("您的 suffix 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_SUFFIX));
            conf.set(Key.SUFFIX, Constant.DEFAULT_SUFFIX);
        } else {
            suffix = suffix.trim();

            if (suffix.contains(File.separator)) {
                throw DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("请检查参数suffix: [%s], 不可以带有路径分隔符 [%s]", suffix, File.separator)
                );
            }

            conf.set(Key.SUFFIX, suffix);
        }

        /*
         * inUseSuffix check
         */
        String inUseSuffix = conf.getString(Key.IN_USE_SUFFIX);

        if (StringUtils.isBlank(inUseSuffix)) {
            LOG.warn(String.format("您的 inUseSuffix 配置为空, 将使用默认值 [%s]", Constant.DEFAULT_IN_USE_SUFFIX));
            conf.set(Key.IN_USE_SUFFIX, Constant.DEFAULT_IN_USE_SUFFIX);
        } else {
            inUseSuffix = inUseSuffix.trim();

            if (inUseSuffix.contains(File.separator)) {
                throw DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("请检查参数inUseSuffix: [%s], 不可以带有路径分隔符 [%s]", inUseSuffix, File.separator)
                );
            }

            conf.set(Key.IN_USE_SUFFIX, inUseSuffix);
        }

        //
        // warn: suffix and inUseSuffix must be different
        //
        if (inUseSuffix.equals(suffix)) {
            throw DataXException.asDataXException(
                    TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("suffix: [%s] 与 inUseSuffix: [%s] 不能相同", suffix, inUseSuffix)
            );
        }
    }

    // not blank
    @Nonnull
    private final String prefixWithUUID;

    @Nonnull
    private final File baseDirectory;

    private final AtomicInteger fileIndex = new AtomicInteger();

    private File currentFile;

    public FileManager(@Nonnull Configuration conf) {
        String path = conf.getString(Key.PATH);

        this.prefixWithUUID = conf.getString(Key.INTERNAL_PREFIX_WITH_UUID);
        this.suffix = conf.getString(Key.SUFFIX);
        this.inUseSuffix = conf.getString(Key.IN_USE_SUFFIX);
        this.baseDirectory = new File(path);
    }

    @Nonnull
    private File nextFile() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.prefixWithUUID);
        sb.append("-");
        sb.append(this.fileIndex.incrementAndGet());
        sb.append(".").append(this.suffix);
        sb.append(".").append(this.inUseSuffix);

        File file = new File(this.baseDirectory, sb.toString());
        this.currentFile = file;  // 不要忘了赋值
        return file;
    }

    @Nonnull
    public File getCurrentFile() {
        File tempFile = this.currentFile;
        return (tempFile == null) ? nextFile() : tempFile;
    }

    public void rotate() {
        File tempFile = this.currentFile;

        if (tempFile != null) {
            this.currentFile = null;

            String tempFilePath = tempFile.getAbsolutePath();
            String destFilePath = tempFilePath.substring(0, tempFilePath.lastIndexOf(this.inUseSuffix) - 1);

            File destFile = new File(destFilePath);

            try {
                FileUtils.moveFile(tempFile, destFile);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("移动文件成功, from: [%s], to: [%s]", tempFilePath, destFilePath));
                }
            } catch (IOException ioe) {
                LOG.error(String.format("移动文件失败, from: [%s], to: [%s]", tempFilePath, destFilePath), ioe);
            }
        }
    }
}
