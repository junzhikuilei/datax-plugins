package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jcraft.jsch.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;

import javax.annotation.Nonnull;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * @author JiaKun Xu, 2023-02-17 11:15
 */
public class SftpHelperImpl extends BaseFtpHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelperImpl.class);

    private Session session;
    private ChannelSftp channelSftp;

    public SftpHelperImpl(@Nonnull Configuration conf) {
        super(conf);

        Assert.assertTrue(Constant.PROTOCOL_SFTP.equalsIgnoreCase(super.protocol));
    }

    @Override
    public void loginFtpServer() {
        final String host = super.host;
        final int port = super.port;
        final int timeout = super.timeout;
        final String username = super.username;
        final String password = super.password;

        JSch jsch = new JSch();
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            session = jsch.getSession(username, host, port);
            if (session == null) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.FAIL_LOGIN,
                        "无法通过sftp传输协议与服务器建立链接, 请检查主机名和用户名是否正确"
                );
            }
            session.setPassword(password);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            // config.put("PreferredAuthentications", "password");
            session.setConfig(config);
            session.setTimeout(timeout);
            session.connect();

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            // warn: 必须在上面连接成功后再赋值，因为ftp roll writer会retry 3次，否则会造成连接浪费
            this.session = session;
            this.channelSftp = channelSftp;
        } catch (JSchException jse) {
            if (null != jse.getCause()) {
                String cause = jse.getCause().toString();

                String wrongHost = "java.net.UnknownHostException: " + host;
                String wrongPort = "java.lang.IllegalArgumentException: port out of range:" + port;
                String connectionRefused = "java.net.ConnectException: Connection refused";

                if (wrongHost.equals(cause)) {
                    String errMsg = String.format(
                            "请确认sftp服务器地址是否正确 [%s], errMsg: %s", host, jse.getMessage()
                    );
                    LOG.error(errMsg);
                    throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg, jse);
                } else if (wrongPort.equals(cause) || connectionRefused.equals(cause)) {
                    String message = String.format(
                            "请确认sftp服务器端口是否正确 [%d], errMsg: %s", port, jse.getMessage()
                    );
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            FtpRollWriterErrorCode.FAIL_LOGIN, message, jse
                    );
                }
            } else {
                String errMsg = String.format(
                        "与sftp服务器建立连接失败, host: [%s], port: [%s], username: [%s], errMsg: %s",
                        host, port, username, jse.getMessage()
                );
                LOG.error(errMsg);
                throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg);
            }
        }
    }

    @Override
    public void logoutFtpServer() {
        Session session = this.session;
        ChannelSftp channelSftp = this.channelSftp;

        if (channelSftp != null) {
            this.channelSftp = null;
            channelSftp.disconnect();
        }

        if (session != null) {
            this.session = null;
            session.disconnect();
        }
    }

    /**
     * sftp server不支持递归创建目录, 只能一级一级创建
     */
    @Override
    public void mkDirRecursive(@Nonnull String directoryPath) {
        // 要创建的目录是否已经存在？
        SftpATTRS attr = null;

        try {
            attr = this.channelSftp.lstat(directoryPath);
        } catch (SftpException ignored) {
        }

        if (attr == null) {
            LOG.info(String.format("递归创建目录 [%s]", directoryPath));
        } else {
            if (attr.isDir()) {
                return;
            } else {
                String errMsg = String.format("目标路径 [%s] 已存在但不是一个目录", directoryPath);
                throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg);
            }
        }

        // 递归创建目录
        StringBuilder pathBuilder = new StringBuilder();
        String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
        int numDirs = dirSplit.length;

        try {
            // sftp server不支持递归创建目录,只能一级一级创建
            for (int i = 0; i < numDirs; ++i) {
                String dirName = dirSplit[i];

                // warn: 已知directory path必是绝对路径，才能这样处理
                if (i == 1) {
                    if (pathBuilder.length() == 1) {
                        pathBuilder.append(dirName);
                    } else {
                        pathBuilder.append(IOUtils.DIR_SEPARATOR_UNIX).append(dirName);
                    }
                } else {
                    pathBuilder.append(IOUtils.DIR_SEPARATOR_UNIX).append(dirName);
                }

                this.mkDirSingleHierarchy(pathBuilder.toString());
            }
        } catch (SftpException se) {
            String errMsg = String.format(
                    "递归创建目录 [%s] 时发生异常, 请确认与sftp服务器的连接正常, 有目录 [-wx] 权限, errMsg: %s",
                    directoryPath, se.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                    errMsg,
                    se
            );
        }
    }

    @Nonnull
    @Override
    public OutputStream getOutputStream(@Nonnull String filePath) {
        try {
            // warn: 已知file path是绝对路径，可以不用change working directory

            // this.printWorkingDirectory();
            // String parentDirPath = filePath.substring(0, filePath.lastIndexOf(IOUtils.DIR_SEPARATOR));
            // channelSftp.cd(parentDirPath);
            // this.printWorkingDirectory();

            return this.channelSftp.put(filePath, ChannelSftp.APPEND);
        } catch (SftpException se) {
            String errMsg = String.format(
                    "打开文件 [%s] 获取写入流时出错, 请确认与sftp服务器的连接正常, 有目录 [r-x] 权限, 有文件 [-w-] 权限 errMsg: %s",
                    filePath, se.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.OPEN_FILE_ERROR,
                    errMsg,
                    se
            );
        }
    }

    @Nonnull
    @Override
    public Set<String> getAllFilesInDir(@Nonnull String dir, @Nonnull String prefixFileName) {
        this.printWorkingDirectory();

        Set<String> matchedRfn = new HashSet<>();

        try {
            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> allEntries = this.channelSftp.ls(dir);

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("ls: %s", JSON.toJSONString(allEntries, SerializerFeature.UseSingleQuotes)));
            }

            for (ChannelSftp.LsEntry entry : allEntries) {
                String rfn = entry.getFilename();
                if (rfn.startsWith(prefixFileName)) {
                    matchedRfn.add(rfn);
                }
            }
        } catch (SftpException se) {
            String errMsg = String.format(
                    "获取目录 [%s] 下的文件列表时出错, 请确认与sftp服务器的连接正常, 有目录 [r-x] 权限, errMsg: %s",
                    dir, se.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                    errMsg,
                    se
            );
        }

        return matchedRfn;
    }

    @Override
    public void deleteFiles(@Nonnull Set<String> filesToDelete) {
        if (filesToDelete.isEmpty()) {
            return;
        }

        ChannelSftp channelSftp = this.channelSftp;
        String deleting = null;

        try {
            for (String each : filesToDelete) {
                deleting = each;

                LOG.info(String.format("delete file: [%s]", each));
                channelSftp.rm(each);
            }
        } catch (SftpException se) {
            String errMsg = String.format(
                    "删除文件 [%s] 时发生异常, 请确认与sftp服务器的连接正常, 有目录 [-wx] 权限, errMsg: %s",
                    deleting, se.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                    errMsg,
                    se
            );
        }
    }

    @Override
    public void rename(@Nonnull String oldPath, @Nonnull String newPath) {
        String msg = String.format("移动文件from: [%s], to: [%s]", oldPath, newPath);
        LOG.debug(msg);  // debug log

        try {
            this.channelSftp.rename(oldPath, newPath);
        } catch (SftpException se) {
            String errMsg = String.format("%s 时发生异常, errMsg: %s", msg, se.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg, se);
        }
    }

    private void mkDirSingleHierarchy(@Nonnull String dirPath) throws SftpException {
        ChannelSftp channelSftp = this.channelSftp;
        SftpATTRS attr = null;

        try {
            attr = channelSftp.lstat(dirPath);
        } catch (SftpException ignored) {
        }

        if (attr == null) {
            LOG.info(String.format("逐级创建目录 [%s]", dirPath));
        } else {
            if (attr.isDir()) {
                return;  // exists, so already created
            } else {
                String errMsg = String.format("中间路径 [%s] 已存在但不是一个目录", dirPath);
                throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg);
            }
        }

        channelSftp.mkdir(dirPath);
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory: [%s]", this.channelSftp.pwd()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error: %s", e.getMessage()));
        }
    }
}
