package com.aliyun.dts.deliver.commons.util;

import java.util.Collection;
import java.util.function.Function;

public class PrettyOutput {
    private static final char LINE_IDENT = '\t';
    private static final char LINE_SEPARATOR = '\n';
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private StringBuilder buffer = new StringBuilder(DEFAULT_BUFFER_SIZE);
    private int identLevel = 0;
    private boolean lineBegin = true;

    public PrettyOutput begin() {
        buffer.setLength(0);
        return this;
    }

    private void addIdentIfNeeded() {
        if (lineBegin) {
            for (int i = 0; i < identLevel; i++) {
                buffer.append(LINE_IDENT);
            }
            lineBegin = false;
        }
    }

    public PrettyOutput addLine() {
        buffer.append(LINE_SEPARATOR);
        lineBegin = true;
        return this;
    }

    public PrettyOutput increaseIdent() {
        identLevel++;
        return this;
    }

    public PrettyOutput decreaseIdent() {
        identLevel--;
        return this;
    }

    public PrettyOutput addContent(Object content) {
        if (null == content) {
            return this;
        }

        addIdentIfNeeded();
        buffer.append(content);
        return this;
    }

    public <T, R> PrettyOutput addContent(Collection<T> content, Function<T, R> contentExtractor) {
        if (null == content) {
            return this;
        }

        addIdentIfNeeded();
        for (T obj : content) {
            buffer.append(contentExtractor.apply(obj));
        }
        return this;
    }

    public PrettyOutput addContent(String content) {
        if (null == content) {
            return this;
        }

        addIdentIfNeeded();
        buffer.append(content);
        return this;
    }

    public PrettyOutput addContent(long content) {
        addIdentIfNeeded();
        buffer.append(content);
        return this;
    }

    public PrettyOutput addCollectionItemPerLine(Collection collection) {
        if (null == collection) {
            return this;
        }

        for (Object item : collection) {
            addContent(item).addLine();
        }
        return this;
    }

    public String getOutput() {
        return buffer.toString();
    }
}
