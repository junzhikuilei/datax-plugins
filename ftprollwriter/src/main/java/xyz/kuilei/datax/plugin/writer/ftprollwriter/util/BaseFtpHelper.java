package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.FtpRollWriterErrorCode;
import xyz.kuilei.datax.plugin.writer.ftprollwriter.Key;

import javax.annotation.Nonnull;
import java.io.OutputStream;
import java.util.Set;

/**
 * @author JiaKun Xu, 2023-02-16 10:48
 */
public abstract class BaseFtpHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BaseFtpHelper.class);

    // not blank
    @Nonnull
    protected final String protocol;

    // not blank
    @Nonnull
    protected final String host;

    // [1, 65535]
    protected final int port;

    // >= 0
    protected final int timeout;

    // not blank
    @Nonnull
    protected final String username;

    // not blank
    @Nonnull
    protected final String password;

    public static void validateParameter(@Nonnull Configuration conf) {
        String protocol = conf.getNecessaryValue(Key.PROTOCOL, FtpRollWriterErrorCode.REQUIRED_VALUE);
        String host = conf.getNecessaryValue(Key.HOST, FtpRollWriterErrorCode.REQUIRED_VALUE);
        Integer port = conf.getInt(Key.PORT);
        Integer timeout = conf.getInt(Key.TIMEOUT);
        String username = conf.getNecessaryValue(Key.USERNAME, FtpRollWriterErrorCode.REQUIRED_VALUE);
        String password = conf.getNecessaryValue(Key.PASSWORD, FtpRollWriterErrorCode.REQUIRED_VALUE);

        /*
         * protocol check
         */
        protocol = protocol.trim();

        if (Constant.PROTOCOL_FTP.equalsIgnoreCase(protocol)) {
            protocol = Constant.PROTOCOL_FTP;

            if (null == port) {
                port = Constant.PORT_FTP;
                LOG.warn(String.format("您没有配置 ftp 端口号, 使用默认值 [%d]", port));
            }
        } else if (Constant.PROTOCOL_SFTP.equalsIgnoreCase(protocol)) {
            protocol = Constant.PROTOCOL_SFTP;

            if (null == port) {
                port = Constant.PORT_SFTP;
                LOG.warn(String.format("您没有配置 sftp 端口号, 使用默认值 [%d]", port));
            }
        } else {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅支持 ftp, sftp 传输协议, 不支持您配置的传输协议 [%s]", protocol)
            );
        }

        conf.set(Key.PROTOCOL, protocol);

        /*
         * host check
         */
        host = host.trim();

        conf.set(Key.HOST, host);

        /*
         * port check
         */
        Assert.assertNotNull(port);

        if (port < 1 || port > 65535) {
            throw DataXException.asDataXException(
                    FtpRollWriterErrorCode.ILLEGAL_VALUE,
                    String.format("您配置的端口号 [%d] 不在端口号范围 [1, 65535]", port)
            );
        }

        conf.set(Key.PORT, port);

        /*
         * timeout check
         */
        if (timeout == null) {
            LOG.warn(String.format("您没有配置连接超时时间(毫秒), 使用默认值 [%d]", Constant.DEFAULT_TIMEOUT));
            timeout = Constant.DEFAULT_TIMEOUT;
        } else {
            if (timeout < 0) {
                throw DataXException.asDataXException(
                        FtpRollWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的连接超时时间(毫秒) [%d] 不是正整数", timeout)
                );
            }
        }

        conf.set(Key.TIMEOUT, timeout);

        /*
         * username check
         */
        username = username.trim();

        conf.set(Key.USERNAME, username);

        /*
         * password check
         */
        // warn: never trim password
    }

    @Nonnull
    public static BaseFtpHelper getImpl(@Nonnull Configuration conf) {
        String protocol = conf.getString(Key.PROTOCOL);

        if (Constant.PROTOCOL_FTP.equalsIgnoreCase(protocol)) {
            return new StandardFtpHelperImpl(conf);
        } else if (Constant.PROTOCOL_SFTP.equalsIgnoreCase(protocol)) {
            return new SftpHelperImpl(conf);
        } else {
            throw new IllegalStateException("should not happen");
        }
    }

    protected BaseFtpHelper(@Nonnull Configuration conf) {
        this.protocol = conf.getString(Key.PROTOCOL);
        this.host = conf.getString(Key.HOST);
        this.port = conf.getInt(Key.PORT);
        this.timeout = conf.getInt(Key.TIMEOUT);
        this.username = conf.getString(Key.USERNAME);
        this.password = conf.getString(Key.PASSWORD);
    }

    // ------------------------------------------------
    // abstract methods
    // ------------------------------------------------
    public abstract void loginFtpServer();

    public abstract void logoutFtpServer();

    /**
     * @param directoryPath not blank
     */
    public abstract void mkDirRecursive(@Nonnull String directoryPath);

    /**
     * @param filePath not blank
     *                 never in the root directory
     */
    @Nonnull
    public abstract OutputStream getOutputStream(@Nonnull String filePath);

    /**
     * 列出目录下的所有文件名，文件是广义上的文件
     *
     * @param dir not blank
     * @param prefixFileName not blank
     * @return maybe empty
     */
    @Nonnull
    public abstract Set<String> getAllFilesInDir(@Nonnull String dir, @Nonnull String prefixFileName);

    /**
     * @param filesToDelete not blank
     */
    public abstract void deleteFiles(@Nonnull Set<String> filesToDelete);

    /**
     * @param oldPath not blank
     * @param newPath not blank
     */
    public abstract void rename(@Nonnull String oldPath, @Nonnull String newPath);
}
