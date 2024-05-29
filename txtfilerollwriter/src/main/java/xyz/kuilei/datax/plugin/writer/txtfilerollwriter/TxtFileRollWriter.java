package xyz.kuilei.datax.plugin.writer.txtfilerollwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util.BaseUnstructuredWriter;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util.DataXRecordReader;
import xyz.kuilei.datax.plugin.writer.txtfilerollwriter.util.FileManager;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * NOTE: 在实现上高度参考了 ftpwriter，以及 flume 中 sink 用到的 path manager
 *
 * @author JiaKun Xu, 2023-02-14 10:44
 */
public class TxtFileRollWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();

            TxtFileRollWriter.validateParameter(conf);
            DataXRecordReader.validateParameter(conf);
            BaseUnstructuredWriter.validateParameter(conf);
            FileManager.validateParameter(conf);

            this.writerSliceConfig = conf;
        }

        /**
         * prepare 之后 split
         *
         * TODO NOTE:
         * job 作为 master, 在分布式模式下只有 1 个节点运行, 如果有多个节点被分配到了 task, 那么不能做到正确 write mode
         *   从 datax 源码来看, 只有 standalone 模式, 即为 job/task 都在同 1 个节点, 因此可以正确
         */
        @Override
        public void prepare() {
            final Configuration conf = writerSliceConfig;
            final String path = conf.getString(Key.PATH);
            final String prefix = conf.getString(Key.PREFIX);
            final String writeMode = conf.getString(Key.WRITE_MODE);

            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容", path, prefix));

                File dir = new File(path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    if (dir.exists()) {
                        // warn:不要使用FileUtils.deleteQuietly(dir);
                        FilenameFilter filter = new PrefixFileFilter(prefix);
                        File[] filesWithPrefix = dir.listFiles(filter);

                        if (filesWithPrefix != null && filesWithPrefix.length != 0) {
                            for (File eachFile : filesWithPrefix) {
                                LOG.info(String.format("delete file: [%s]", eachFile.getName()));
                                FileUtils.forceDelete(eachFile);
                            }
                        }
                    }
                } catch (NullPointerException npe) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("您配置的目录在清空时出现空指针异常 [%s]", path),
                            npe
                    );
                } catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 [%s]", path),
                            iae
                    );
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 [%s]", path),
                            se
                    );
                } catch (IOException ioe) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("无法清空目录 [%s]", path),
                            ioe
                    );
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀 [%s] 的文件", path, prefix));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));

                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw DataXException.asDataXException(
                                    TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                                    String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况", path)
                            );
                        }

                        // prefix is never blank
                        FilenameFilter filter = new PrefixFileFilter(prefix);
                        File[] filesWithPrefix = dir.listFiles(filter);

                        if (filesWithPrefix != null && filesWithPrefix.length != 0) {
                            List<String> allFiles = new ArrayList<>(filesWithPrefix.length);
                            for (File eachFile : filesWithPrefix) {
                                allFiles.add(eachFile.getName());
                            }

                            LOG.error(String.format(
                                    "冲突文件列表为 [%s]", StringUtils.join(allFiles.iterator(), ",")
                            ));

                            throw DataXException.asDataXException(
                                    TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                                    String.format("您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path)
                            );
                        }
                    } else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException.asDataXException(
                                    TxtFileRollWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                    String.format("您指定的文件路径 [%s] 创建失败.", path)
                            );
                        }
                    }
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 [%s]", path),
                            se
                    );
                }
            } else {
                throw DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 [%s]", writeMode)
                );
            }
        }

        @Override
        public void post() {
            // do nothing
        }

        @Override
        public void destroy() {
            // do nothing
        }

        @Override
        public List<Configuration> split(final int mandatoryNumber) {
            final Configuration jobConf = writerSliceConfig;
            final String path = jobConf.getString(Key.PATH);
            final String prefix = jobConf.getString(Key.PREFIX);

            LOG.info("begin do split...");
            // 列出所有文件前缀
            Set<String> allPwu = new HashSet<>();
            final int pwuLen = genPrefixWithUUID(prefix).length();

            try {
                File dir = new File(path);
                String[] allFnm = dir.list();

                if (allFnm != null && allFnm.length != 0) {
                    for (String fnm : allFnm) {
                        int fnmLen = fnm.length();
                        // if (fnmLen < pwuLen) {
                        //     // do nothing
                        // }
                        if (fnmLen == pwuLen) {
                            allPwu.add(fnm);
                        } else if (fnmLen > pwuLen) {
                            allPwu.add(fnm.substring(0, pwuLen));
                        }
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 [%s]", path),
                        se
                );
            }

            // 生成 task 配置
            List<Configuration> allTaskConf = new ArrayList<>(mandatoryNumber);

            for (int i = 0; i < mandatoryNumber; ++i) {
                // warn: 只要文件的前缀名不重复，文件就不会重复
                String prefixWithUUID;
                do {
                    prefixWithUUID = genPrefixWithUUID(prefix);
                } while (allPwu.contains(prefixWithUUID));
                allPwu.add(prefixWithUUID);

                // add itself
                LOG.info(String.format("split 1 write task, prefix with UUID: [%s]", prefixWithUUID));
                final Configuration taskConf = jobConf.clone();
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

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            // do nothing
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            final Configuration conf = this.writerSliceConfig;
            final String path = conf.getString(Key.PATH);
            final String prefixWithUUID = conf.getString(Key.INTERNAL_PREFIX_WITH_UUID);
            final String suffix = conf.getString(Key.SUFFIX);

            LOG.info("begin do write...");
            LOG.info(String.format("write to path: [%s], prefix with UUID: [%s], suffix: [%s]", path, prefixWithUUID, suffix));

            DataXRecordReader recordReader = new DataXRecordReader(conf, lineReceiver);
            BaseUnstructuredWriter baseWriter = BaseUnstructuredWriter.getImpl(conf);

            /*
             * 在 alibaba 的 txt file writer 中: 如果写入文件失败, 那么放到错误记录里面, 而且不会抛出异常
             * 从数据接入的角度来看: 为了不丢数据, 如果写入文件失败, 那么要抛出异常让 datax 任务失败
             */
            DataXException de = null;

            try {
                String[] splitRows;
                while ((splitRows = recordReader.readOneRecord()) != null) {
                    baseWriter.writeOneRecord(splitRows);
                }
            } catch (SecurityException se) {
                de = DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                        "没有权限创建文件",
                        se
                );
            } catch (IOException ioe) {
                de = DataXException.asDataXException(
                        TxtFileRollWriterErrorCode.WRITE_FILE_IO_ERROR,
                        "写入文件失败",
                        ioe
                );
            } finally {
                try {
                    baseWriter.close();
                } catch (IOException ioe) {
                    if (de == null) {
                        de = DataXException.asDataXException(
                                TxtFileRollWriterErrorCode.WRITE_FILE_IO_ERROR,
                                "关闭文件失败",
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
            // do nothing
        }
    }

    public static void validateParameter(@Nonnull Configuration conf) {
        String path = conf.getNecessaryValue(Key.PATH, TxtFileRollWriterErrorCode.REQUIRED_VALUE);
        String writeMode = conf.getNecessaryValue(Key.WRITE_MODE, TxtFileRollWriterErrorCode.REQUIRED_VALUE);
        String prefix = conf.getNecessaryValue(Key.PREFIX, TxtFileRollWriterErrorCode.REQUIRED_VALUE);

        /*
         * path check
         */
        path = path.trim();

        File dir = new File(path);
        try {
            if (dir.exists()) {
                if (!dir.isDirectory()) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 已存在但不是一个目录", path)
                    );
                }
            } else {
                boolean createdOk = dir.mkdirs();
                if (!createdOk) {
                    throw DataXException.asDataXException(
                            TxtFileRollWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("您配置的path: [%s] 创建目录失败", path)
                    );
                }
            }
        } catch (SecurityException se) {
            throw DataXException.asDataXException(
                    TxtFileRollWriterErrorCode.SECURITY_NOT_ENOUGH,
                    String.format("您没有权限创建目录路径 [%s] ", path),
                    se
            );
        }

        path = dir.getAbsolutePath();  // 借助file system规范路径格式
        conf.set(Key.PATH, path);

        /*
         * writeMode check
         */
        writeMode = writeMode.trim();

        Set<String> supportedWriteModes = new HashSet<>(Arrays.asList("truncate", "append", "nonConflict"));
        if (!supportedWriteModes.contains(writeMode)) {
            throw DataXException.asDataXException(
                    TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 [%s]", writeMode)
            );
        }

        conf.set(Key.WRITE_MODE, writeMode);

        /*
         * prefix check
         */
        prefix = prefix.trim();

        if (prefix.contains(File.separator)) {
            throw DataXException.asDataXException(
                    TxtFileRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("请检查参数prefix: [%s], 不可以带有路径分隔符 [%s]", prefix, File.separator)
            );
        }

        conf.set(Key.PREFIX, prefix);
    }
}
