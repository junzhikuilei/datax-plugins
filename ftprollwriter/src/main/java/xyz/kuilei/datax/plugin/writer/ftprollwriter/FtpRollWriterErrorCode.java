package xyz.kuilei.datax.plugin.writer.ftprollwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author JiaKun Xu, 2023-02-15 15:29
 */
public enum FtpRollWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("FtpRollWriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("FtpRollWriter-01", "您填写的参数值不合法."),
    MIXED_INDEX_VALUE("FtpRollWriter-02", "您的列信息配置同时包含了index,value."),
    NO_INDEX_VALUE("FtpRollWriter-03", "您明确的配置列信息,但未填写相应的index,value."),

    FILE_NOT_EXISTS("FtpRollWriter-04", "您配置的目录文件路径不存在或者没有权限读取."),
    OPEN_FILE_WITH_CHARSET_ERROR("FtpRollWriter-05", "您配置的文件编码和实际文件编码不符合."),
    OPEN_FILE_ERROR("FtpRollWriter-06", "您配置的文件在打开时异常."),
    WRITE_FILE_IO_ERROR("FtpRollWriter-07", "您配置的文件在读取时出现IO异常."),
    SECURITY_NOT_ENOUGH("FtpRollWriter-08", "您缺少权限执行相应的文件操作."),
    CONFIG_INVALID_EXCEPTION("FtpRollWriter-09", "您的参数配置错误."),
    RUNTIME_EXCEPTION("FtpRollWriter-10", "出现运行时异常, 请联系我们"),
    EMPTY_DIR_EXCEPTION("FtpRollWriter-11", "您尝试读取的文件目录为空."),

    FAIL_LOGIN("FtpRollWriter-12", "登录失败,无法与ftp服务器建立连接."),
    FAIL_DISCONNECT("FtpRollWriter-13", "关闭ftp连接失败,无法与ftp服务器断开连接."),
    COMMAND_FTP_IO_EXCEPTION("FtpRollWriter-14", "与ftp服务器连接异常."),
    OUT_MAX_DIRECTORY_LEVEL("FtpRollWriter-15", "超出允许的最大目录层数."),
    LINK_FILE("FtpRollWriter-16", "您尝试读取的文件为链接文件."),
    COMMAND_FTP_ENCODING_EXCEPTION("FtpRollWriter-17", "与ftp服务器连接，使用指定编码异常."),
    FAIL_LOGOUT("FtpRollWriter-18", "登出失败,关闭与ftp服务器建立连接失败,但这不影响任务同步."),;

    private final String code;
    private final String description;

    FtpRollWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
