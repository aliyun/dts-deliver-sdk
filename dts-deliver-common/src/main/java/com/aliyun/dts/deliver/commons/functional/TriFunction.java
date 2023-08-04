package com.aliyun.dts.deliver.commons.functional;

import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface TriFunction<T, U, N, R> {

    R apply(T t, U u, N n);

    default <V> TriFunction<T, U, N, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (T t, U u, N n) -> after.apply(apply(t, u, n));
    }
}
