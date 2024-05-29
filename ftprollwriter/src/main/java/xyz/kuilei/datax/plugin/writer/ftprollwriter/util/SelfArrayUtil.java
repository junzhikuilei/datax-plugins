package xyz.kuilei.datax.plugin.writer.ftprollwriter.util;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author JiaKun Xu, 2023-02-27 18:53
 */
public class SelfArrayUtil {
    public static String[] toStringArray(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        final String[] result = new String[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = String.valueOf(array[i]);
        }

        return result;
    }
}
