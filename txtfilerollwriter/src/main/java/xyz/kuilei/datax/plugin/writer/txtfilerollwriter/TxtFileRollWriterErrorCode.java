package xyz.kuilei.datax.plugin.writer.txtfilerollwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author JiaKun Xu, 2023-02-14 10:44
 */
public enum TxtFileRollWriterErrorCode implements ErrorCode {
    CONFIG_INVALID_EXCEPTION("TxtFileRollWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("TxtFileRollWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("TxtFileRollWriter-02", "您填写的参数值不合法."),
    WRITE_FILE_ERROR("TxtFileRollWriter-03", "您配置的目标文件在写入时异常."),
    WRITE_FILE_IO_ERROR("TxtFileRollWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("TxtFileRollWriter-05", "您缺少权限执行相应的文件写入操作.");

    private final String code;
    private final String description;

    TxtFileRollWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", code, description);
    }
}
