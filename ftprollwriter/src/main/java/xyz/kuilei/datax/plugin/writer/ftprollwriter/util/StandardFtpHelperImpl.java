package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author JiaKun Xu, 2023-02-16 19:59
 */
public class StandardFtpHelperImpl extends BaseFtpHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelperImpl.class);

    private FTPClient ftpClient;

    public StandardFtpHelperImpl(@Nonnull Configuration conf) {
        super(conf);

        Assert.assertTrue(Constant.PROTOCOL_FTP.equalsIgnoreCase(super.protocol));
    }

    @Override
    public void loginFtpServer() {
        final String host = super.host;
        final int port = super.port;
        final int timeout = super.timeout;
        final String username = super.username;
        final String password = super.password;

        FTPClient ftpClient = new FTPClient();

        try {
            ftpClient.setControlEncoding("UTF-8");
            // 不需要写死ftp server的OS TYPE, FTPClient getSystemType()方法会自动识别
            // ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
            ftpClient.setDefaultTimeout(timeout);
            ftpClient.setConnectTimeout(timeout);
            ftpClient.setDataTimeout(timeout);

            // 连接登录
            ftpClient.connect(host, port);
            ftpClient.login(username, password);

            ftpClient.enterRemotePassiveMode();
            ftpClient.enterLocalPassiveMode();

            // ftp服务器是否接受咱的连接请求
            int replyCode = ftpClient.getReplyCode();

            if (FTPReply.isPositiveCompletion(replyCode)) {
                this.ftpClient = ftpClient;
            } else {
                try {
                    ftpClient.disconnect();
                } catch (IOException ignored) {
                }

                String errMsg = String.format(
                        "与ftp服务器建立连接失败, host: [%s], port: [%s], username: [%s], replyCode: [%s]",
                        host, port, username, replyCode
                );
                LOG.error(errMsg);
                throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg);
            }
        } catch (UnknownHostException uhe) {
            String errMsg = String.format("请确认ftp服务器地址是否正确 [%s], errMsg: %s", host, uhe.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg, uhe);
        } catch (IllegalArgumentException iae) {
            String errMsg = String.format("请确认ftp服务器端口是否正确 [%d], errMsg: %s", port, iae.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg, iae);
        } catch (Exception e) {
            String errMsg = String.format(
                    "与ftp服务器建立连接失败, host: [%s], port: [%s], username: [%s], errMsg: %s",
                    host, port, username, e.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_LOGIN, errMsg, e);
        }
    }

    @Override
    public void logoutFtpServer() {
        FTPClient ftpClient = this.ftpClient;

        if (ftpClient.isConnected()) {
            this.ftpClient = null;

            // logout & disconnect
            DataXException de = null;

            try {
                ftpClient.logout();
            } catch (IOException ioe) {
                String errMsg = String.format("注销ftp服务器登录失败, errMsg: %s", ioe.getMessage());
                LOG.error(errMsg);
                de = DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_DISCONNECT, errMsg, ioe);
            }

            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (IOException ioe) {
                    String errMsg = String.format("断开ftp服务器连接失败, errMsg: %s", ioe.getMessage());
                    LOG.error(errMsg);
                    if (de == null) {
                        de = DataXException.asDataXException(FtpRollWriterErrorCode.FAIL_DISCONNECT, errMsg, ioe);
                    }
                }
            }

            if (de != null) {
                throw de;
            }
        }
    }

    @Override
    public void mkDirRecursive(@Nonnull String directoryPath) {
        // 要创建的目录是否已经存在？
        boolean directoryExists = false;

        try {
            directoryExists = this.ftpClient.changeWorkingDirectory(directoryPath);
        } catch (IOException ignored) {
        }

        if (directoryExists) {
            return;
        } else {
            LOG.info(String.format("递归创建目录 [%s]", directoryPath));
        }

        // 递归创建目录
        StringBuilder pathBuilder = new StringBuilder();
        String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
        int numDirs = dirSplit.length;

        String errMsg = String.format(
                "递归创建目录 [%s] 时发生异常, 请确认与ftp服务器的连接正常, 有目录 [-wx] 权限",
                directoryPath
        );

        try {
            // ftp server不支持递归创建目录,只能一级一级创建
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

                boolean mkDirSuccess = this.mkDirSingleHierarchy(pathBuilder.toString());
                if (!mkDirSuccess) {
                    throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg);
                }
            }
        } catch (IOException ioe) {
            errMsg = String.format("%s, errMsg: %s", errMsg, ioe.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg, ioe);
        }
    }

    @Nonnull
    @Override
    public OutputStream getOutputStream(@Nonnull String filePath) {
        FTPClient ftpClient = this.ftpClient;

        String errMsg = String.format(
                "打开文件 [%s] 获取写入流时出错, 请确认与ftp服务器的连接正常, 有目录 [r-x] 权限, 有文件 [-w-] 权限",
                filePath
        );

        try {
            // warn: 已知file path是绝对路径，可以不用change working directory

            // this.printWorkingDirectory();
            // String parentDir = filePath.substring(0, filePath.lastIndexOf(IOUtils.DIR_SEPARATOR_UNIX));
            // ftpClient.changeWorkingDirectory(parentDir);
            // this.printWorkingDirectory();

            OutputStream out = ftpClient.appendFileStream(filePath);
            if (null == out) {
                throw DataXException.asDataXException(FtpRollWriterErrorCode.OPEN_FILE_ERROR, errMsg);
            }
            return out;
        } catch (IOException ioe) {
            errMsg = String.format("%s, errMsg: %s", errMsg, ioe.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.OPEN_FILE_ERROR, errMsg, ioe);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory: [%s]", this.ftpClient.printWorkingDirectory()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error: %s", e.getMessage()));
        }
    }

    @Nonnull
    @Override
    public Set<String> getAllFilesInDir(@Nonnull String dir, @Nonnull String prefixFileName) {
        FTPClient ftpClient = this.ftpClient;

        Set<String> allFilesWithPointedPrefix = new HashSet<>();
        try {
            boolean isDirExist = ftpClient.changeWorkingDirectory(dir);
            if (!isDirExist) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                        String.format("进入目录 [%s] 失败, 请确认与ftp服务器的连接正常, 有目录 [r-x] 权限", dir)
                );
            }

            this.printWorkingDirectory();

            FTPFile[] fs = ftpClient.listFiles(dir);
            if (LOG.isDebugEnabled()) {
                // LOG.debug(JSON.toJSONString(this.ftpClient.listNames(dir)));
                LOG.debug(String.format("ls: %s", JSON.toJSONString(fs, SerializerFeature.UseSingleQuotes)));
            }
            for (FTPFile ff : fs) {
                String ffName = ff.getName();
                if (ffName.startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(ffName);
                }
            }
        } catch (IOException ioe) {
            String message = String.format(
                    "获取目录 [%s] 的文件列表时发生异常, 请确认与ftp服务器的连接正常, 有目录 [r-x] 权限, errMsg: %s",
                    dir, ioe.getMessage()
            );
            LOG.error(message);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, message, ioe);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(@Nonnull Set<String> filesToDelete) {
        if (filesToDelete.isEmpty()) {
            return;
        }

        FTPClient ftpClient = this.ftpClient;
        String deleting = null;
        boolean deleteOk = false;

        try {
            for (String each : filesToDelete) {
                deleting = each;

                LOG.info(String.format("delete file: [%s].", each));
                deleteOk = ftpClient.deleteFile(each);
                if (!deleteOk) {
                    String message = String.format("删除文件 [%s] 时失败, 请确认有权限删除", deleting);
                    throw DataXException.asDataXException(
                            FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                            message
                    );
                }
            }
        } catch (IOException ioe) {
            String errMsg = String.format(
                    "删除文件 [%s] 时发生异常, 请确认有权限删除, 以及网络交互正常, errMsg: %s",
                    deleting, ioe.getMessage()
            );
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg, ioe);
        }
    }

    @Override
    public void rename(@Nonnull String oldPath, @Nonnull String newPath) {
        this.completePendingCommand();

        FTPClient ftpClient = this.ftpClient;

        String msg = String.format("移动文件from: [%s], to: [%s]", oldPath, newPath);
        LOG.debug(msg);  // debug log

        boolean moveOk = false;
        try {
            moveOk = ftpClient.rename(oldPath, newPath);
            if (!moveOk) {
                int replyCode = ftpClient.getReplyCode();

                String errMsg = String.format("%s 失败, replyCode: [%d]", msg, replyCode);
                LOG.error(errMsg);
                throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg);
            }
        } catch (IOException ioe) {
            String errMsg = String.format("%s 时发生异常, errMsg: %s", msg, ioe.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg, ioe);
        }
    }

    private boolean mkDirSingleHierarchy(String directoryPath) throws IOException {
        FTPClient ftpClient = this.ftpClient;

        boolean isDirExist = ftpClient.changeWorkingDirectory(directoryPath);
        // 如果directoryPath目录不存在,则创建
        if (!isDirExist) {
            int reply = ftpClient.mkd(directoryPath);
            if (reply != FTPReply.COMMAND_OK && reply != FTPReply.PATHNAME_CREATED) {
                return false;
            }
        }

        return true;
    }

    private void completePendingCommand() {
        /*
         * Q:After I perform a file transfer to the server,
         * printWorkingDirectory() returns null. A:You need to call
         * completePendingCommand() after transferring the file. wiki:
         * http://wiki.apache.org/commons/Net/FrequentlyAskedQuestions
         */
        String errMsg = "完成ftp completePendingCommand操作时发生异常";

        try {
            boolean isOk = this.ftpClient.completePendingCommand();
            if (!isOk) {
                throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg);
            }
        } catch (IOException ioe) {
            errMsg = String.format("%s, errMsg: %s", errMsg, ioe.getMessage());
            LOG.error(errMsg);
            throw DataXException.asDataXException(FtpRollWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, errMsg, ioe);
        }
    }
}
