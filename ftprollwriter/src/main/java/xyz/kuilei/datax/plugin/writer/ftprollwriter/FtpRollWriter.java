package xyz.kuilei.datax.plugin.writer.ftprollwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.BaseFtpHelper;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.BaseRemoteWriter;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.DataXRecordReader;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.util.FilePathManager;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * NOTE: 在实现上高度参考了 ftpwriter，以及 flume 中 sink 用到的 path manager
 *
 * @author JiaKun Xu, 2023-02-15 15:31
 */
public class FtpRollWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig;
        private BaseFtpHelper ftpHelper;

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();

            BaseFtpHelper.validateParameter(conf);
            FtpRollWriter.validateParameter(conf);
            DataXRecordReader.validateParameter(conf);
            BaseRemoteWriter.validateParameter(conf);
            FilePathManager.validateParameter(conf);

            this.writerSliceConfig = conf;

            BaseFtpHelper ftpHelper = BaseFtpHelper.getImpl(conf);

            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    ftpHelper.loginFtpServer();
                    return null;
                }, 3, 4000, true);
            } catch (Exception e) {
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                } else {
                    throw DataXException.asDataXException(
                            FtpRollWriterErrorCode.FAIL_LOGIN,
                            "Writer.Job与ftp/sftp服务器建立连接失败",
                            e
                    );
                }
            }

            this.ftpHelper = ftpHelper;
        }

        @Override
        public void prepare() {
            final Configuration conf = this.writerSliceConfig;
            final BaseFtpHelper ftpHelper = this.ftpHelper;

            final String path = conf.getString(Key.PATH);
            final String writeMode = conf.getString(Key.WRITE_MODE);
            final String prefix = conf.getString(Key.PREFIX);

            // warn: 这里用户需要配一个目录
            ftpHelper.mkDirRecursive(path);

            if ("truncate".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s]下面以 [%s] 开头的内容", path, prefix));

                Set<String> existingFN = ftpHelper.getAllFilesInDir(path, prefix);

                Set<String> fullFN2Delete = new HashSet<>();
                for (String each : existingFN) {
                    fullFN2Delete.add(path + "/" + each);
                }

                ftpHelper.deleteFiles(fullFN2Delete);
            } else if ("append".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀 [%s] 的文件",
                        path, prefix
                ));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));

                Set<String> existingFN = ftpHelper.getAllFilesInDir(path, prefix);
                final int existingSize = existingFN.size();

                if (existingSize != 0) {
                    String conflictOne = existingFN.iterator().next();

                    String errMsg = String.format(
                            "目录path: [%s]下存在相同前缀prefix: [%s]的文件或文件夹, 冲突样例 [%s], 冲突个数 [%d]",
                            path, prefix, conflictOne, existingSize
                    );
                    throw DataXException.asDataXException(
                            FtpRollWriterErrorCode.ILLEGAL_VALUE,
                            errMsg
                    );
                }
            } else {
                throw new IllegalStateException("should not happen");
            }
        }

        @Override
        public void post() {
            // do nothing
        }

        @Override
        public void destroy() {
            // warn: 没有用来传输文件，可以忽略它的异常
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception ignored) {
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            final Configuration jobConf = this.writerSliceConfig;
            final BaseFtpHelper ftpHelper = this.ftpHelper;
            final String path = jobConf.getString(Key.PATH);
            final String prefix = jobConf.getString(Key.PREFIX);

            // all remote file name
            Set<String> allRfn = ftpHelper.getAllFilesInDir(path, prefix);
            // all prefix with uuid
            Set<String> allPwu = new HashSet<>();
            final int pwuLen = genPrefixWithUUID(prefix).length();

            for (String rfn : allRfn) {
                final int rfnLen = rfn.length();
                // if (fnmLen < pwuLen) {
                //     // do nothing
                // }
                if (rfnLen == pwuLen) {
                    allPwu.add(rfn);
                } else if (rfnLen > pwuLen) {
                    allPwu.add(rfn.substring(0, pwuLen));
                }
            }

            List<Configuration> allTaskConf = new ArrayList<>(mandatoryNumber);

            for (int i = 0; i < mandatoryNumber; ++i) {
                String prefixWithUUID;
                do {
                    prefixWithUUID = genPrefixWithUUID(prefix);
                } while (allPwu.contains(prefixWithUUID));
                allPwu.add(prefixWithUUID);

                LOG.info(String.format("split 1 write task, prefix with UUID: [%s]", prefixWithUUID));
                Configuration taskConf = jobConf.clone();
                taskConf.set(Key.INTERNAL_PREFIX_WITH_UUID, prefixWithUUID);
                allTaskConf.add(taskConf);
            }

            LOG.info("end do split.");
            return allTaskConf;
        }

        @Nonnull
        private static String genPrefixWithUUID(@Nonnull String prefix) {
            String uuid = UUID.randomUUID().toString()
                    .replace("-", "")
                    .toUpperCase();
            return String.format("%s-%s", prefix, uuid);
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private BaseFtpHelper ftpHelper;

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();
            this.writerSliceConfig = conf;

            BaseFtpHelper ftpHelper = BaseFtpHelper.getImpl(conf);

            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    ftpHelper.loginFtpServer();
                    return null;
                }, 3, 4000, true);
            } catch (Exception e) {
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                } else {
                    throw DataXException.asDataXException(
                            FtpRollWriterErrorCode.FAIL_LOGIN,
                            "Writer.Task与ftp/sftp服务器建立连接失败",
                            e
                    );
                }
            }

            this.ftpHelper = ftpHelper;
        }

        @Override
        public void prepare() {
            // do nothing
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            final Configuration conf = this.writerSliceConfig;
            final BaseFtpHelper ftpHelper = this.ftpHelper;

            final String path = conf.getString(Key.PATH);
            final String prefixWithUUID = conf.getString(Key.INTERNAL_PREFIX_WITH_UUID);
            final String suffix = conf.getString(Key.SUFFIX);

            LOG.info("begin do write...");
            LOG.info(String.format("write to path: [%s], prefix with UUID: [%s], suffix: [%s]", path, prefixWithUUID, suffix));

            DataXRecordReader recordReader = new DataXRecordReader(conf, lineReceiver);
            BaseRemoteWriter remoteWriter = BaseRemoteWriter.getImpl(conf, ftpHelper);

            // write
            DataXException de = null;

            try {
                String[] splitRows;
                while ((splitRows = recordReader.readOneRecord()) != null) {
                    remoteWriter.writeOneRecord(splitRows);
                }
            } catch (IOException ioe) {
                de = DataXException.asDataXException(
                        FtpRollWriterErrorCode.WRITE_FILE_IO_ERROR,
                        "写入ftp/sftp文件失败",
                        ioe
                );
            } finally {
                try {
                    remoteWriter.close();
                } catch (IOException ioe) {
                    if (de == null) {
                        de = DataXException.asDataXException(
                                FtpRollWriterErrorCode.WRITE_FILE_IO_ERROR,
                                "关闭ftp/sftp文件失败",
                                ioe
                        );
                    }
                }
            }

            if (de != null) {
                throw de;
            }

            LOG.info("end do write.");
        }

        @Override
        public void post() {
            // do nothing
        }

        @Override
        public void destroy() {
            // warn: ftp storage writer在close时可以保证断开ftp/sftp连接前把文件写完，因此可以忽略它的异常
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception ignored) {
            }
        }
    }

    public static void validateParameter(@Nonnull Configuration conf) {
        String path = conf.getNecessaryValue(Key.PATH, FtpRollWriterErrorCode.REQUIRED_VALUE);
        String writeMode = conf.getNecessaryValue(Key.WRITE_MODE, FtpRollWriterErrorCode.REQUIRED_VALUE);
        String prefix = conf.getNecessaryValue(Key.PREFIX, FtpRollWriterErrorCode.REQUIRED_VALUE);

        /*
         * path check
         */
        path = path.trim();
        path = path.replaceAll("[/]+", "/");

        if (path.startsWith("/")) {
            if (path.equals("/")) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        "请检查参数path: [/], 不可以配置为根目录"
                );
            }
        } else {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("请检查参数path: [%s], 需要配置为绝对路径", path)
            );
        }

        // 不要以路径分隔符结尾
        if (path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }

        conf.set(Key.PATH, path);

        /*
         * writeMode check
         */
        writeMode = writeMode.trim();

        Set<String> supportedWriteModes = new HashSet<>(Arrays.asList("truncate", "append", "nonConflict"));
        if (!supportedWriteModes.contains(writeMode)) {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 [%s]", writeMode)
            );
        }

        conf.set(Key.WRITE_MODE, writeMode);

        /*
         * prefix check
         */
        prefix = prefix.trim();

        if (prefix.contains("/")) {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("请检查参数prefix: [%s], 不可以带有路径分隔符 [/]", prefix)
            );
        }

        conf.set(Key.PREFIX, prefix);
    }
}
