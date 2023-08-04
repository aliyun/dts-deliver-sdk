package com.aliyun.dts.deliver.commons.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class Settings {
    private static final Logger LOG = LoggerFactory.getLogger(Setting.class);

    private static List<Setting<?>> registeredSettings = new LinkedList<>();
    private final Map<String, Object> settings = new ConcurrentHashMap<>();

    public Settings(Properties settings) {
        Map<String, Object> validSettings = new TreeMap<>();
        if (null != settings) {
            settings.entrySet().forEach(entry -> {
                if (null != entry.getValue()) {
                    validSettings.put(entry.getKey().toString(), entry.getValue());
                }
            });
        }
        this.settings.putAll(validSettings);
    }

    private Settings(Map<String, Object> settings) {
        this.settings.putAll(settings);
    }

    public Settings clone() {
        return new Settings(getSettings());
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    /**
     * Merge @this and @another settings to a new one, @this has the highest priority, which means if @this
     * and @another gets same setting, the new one will use the item from @this.
     *
     * @return new settings merged by @this and @another, if @another is <code>null</code>, return this
     */
    public Settings mergeFrom(Settings another) {
        return seeAnother(another, (oldValue, newValue) -> oldValue);
    }

    /**
     * Override @this with @another properties, if key existed in @this, just replace it with value in @another.
     */
    public Settings overrideWith(Settings another) {
        return seeAnother(another, (oldValue, newValue) -> newValue);
    }

    private Settings seeAnother(Settings another, BinaryOperator<Object> mergeFunction) {
        if (another == null) {
            return this;
        }

        Map<String, Object> mergedSettings = new TreeMap<>();

        settings.forEach((key, value) -> mergedSettings.put(key, value));
        another.settings.forEach((key, anotherValue) ->
            mergedSettings.merge(key, anotherValue, (oldValue, newValue) -> mergeFunction.apply(oldValue, newValue)));

        settings.clear();
        settings.putAll(mergedSettings);
        return this;
    }

    private Object internalGet(String key) {
        return settings.get(key);
    }

    public String get(String key) {
        Object value;

        return (value = internalGet(key)) == null ? null : value.toString();
    }

    public String get(String key, String defaultValue) {
        String value;

        return (value = get(key)) == null ? defaultValue : value;
    }

    public void set(String key, Object value) {
        if (!StringUtils.isEmpty(key) && null != value) {
            settings.put(key, value);
        } else {
            LOG.warn("Ignore setting invalid key {} and value {}", key, value);
        }
    }

    public int getIntegerOrDefault(String key, int defaultValue) {
        String value = get(key);

        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("parse {} failed.cause by {}", defaultValue, e.getMessage());
        }

        return defaultValue;
    }

    public void logAll() {
        LOG.info("all settings is:");
        synchronized (registeredSettings) {
            registeredSettings.sort(Comparator.naturalOrder());
            for (Setting setting : registeredSettings) {
                String value = setting.getRawValueWithEncryptFilter(this);
                LOG.info("\t{}: {}", setting.getKey(), value);
            }
        }
    }

    public static Setting<String> stringSetting(String key, String description) {
        return new Setting<>(key, description, rawValue -> rawValue);
    }

    public static Setting<String> stringSetting(String key, String description,
                                                String defaultValue) {
        return stringSetting(key, description, defaultValue, false);
    }

    public static Setting<String> stringSetting(String key, String description,
                                                String defaultValue, boolean encrypt) {
        return new Setting<>(key, description, rawValue -> rawValue, defaultValue, false, encrypt);
    }

    public static Setting<String> stringSetting(String key, List<String> aliasNames, String description,
                                                String defaultValue, boolean encrypt) {
        return new Setting<>(key, aliasNames, description, rawValue -> rawValue, defaultValue, false, encrypt);
    }

    public static Setting<Boolean> booleanSetting(String key, String description) {
        return new Setting<>(key, description, rawValue -> Boolean.parseBoolean(rawValue));
    }

    public static Setting<Boolean> booleanSetting(String key, String description, Boolean defaultValue) {
        return new Setting<>(key,
            description, rawValue -> Boolean.parseBoolean(rawValue), defaultValue);
    }

    public static Setting<Boolean> booleanSetting(String key, List<String> aliasKeyNames, String description,
                                                  Boolean defaultValue) {
        return new Setting<>(key, aliasKeyNames,
            description, rawValue -> Boolean.parseBoolean(rawValue), defaultValue, false, false);
    }

    public static Setting<Integer> integerSetting(String key, String description) {
        return new Setting<>(key, description, rawValue -> Integer.parseInt(rawValue));
    }

    public static Setting<Integer> integerSetting(String key, String description, Integer defaultValue) {
        return new Setting<>(key,
            description, rawValue -> Integer.parseInt(rawValue), defaultValue);
    }

    public static Setting<Integer> integerSetting(String key, List<String> aliasNames, String description,
                                                  Integer defaultValue) {
        return new Setting<>(key, aliasNames,
            description, rawValue -> Integer.parseInt(rawValue), defaultValue, false, false);
    }

    public static Setting<Double> doubleSetting(String key, String description, Double defaultValue) {
        return new Setting<>(key, description, rawValue -> Double.parseDouble(rawValue), defaultValue);
    }

    public static Setting<Double> doubleSetting(String key, List<String> aliasNames, String description, Double defaultValue) {
        return new Setting<>(key, aliasNames,
            description, rawValue -> Double.parseDouble(rawValue), defaultValue, false, false);
    }

    public static Setting<Long> longSetting(String key, String description, Long defaultValue) {
        return new Setting<>(key,
            description, rawValue -> Long.parseLong(rawValue), defaultValue);
    }

    public static Setting<Long> longSetting(String key, List<String> aliasNames, String description, Long defaultValue) {
        return new Setting<>(key, aliasNames,
            description, rawValue -> Long.parseLong(rawValue), defaultValue, false, false);
    }

    public static Setting<Object> objectSetting(String key, String description, Object defaultValue) {
        return new Setting<>(key, description,
            rawValue -> rawValue,
            defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static <T> Setting<Class<? extends T>> classSetting(String key, String description, Class<? extends T> defaultValue) {
        return new Setting<>(key, description,
            rawValue -> {
                try {
                    return (Class<? extends T>) Class.forName(rawValue);
                } catch (ClassNotFoundException e) {
                    LOG.error("exception while load class for {}", rawValue, e);
                    return null;
                }
            },
            defaultValue);
    }

    public static Setting getSetting(String key) {
        synchronized (registeredSettings) {
            for (Setting setting : registeredSettings) {
                if (setting.isKeyMatch(key)) {
                    return setting;
                }
            }
            return null;
        }
    }

    public static class Setting<T> implements Comparable<Setting> {
        private String key;
        private List<String> aliasKeyNames;
        private String description;
        private boolean hasDefaultValue;
        private final T defaultValue;
        private final Function<String, T> parser;
        private final boolean required;
        private boolean encrypt = false;
        private Map<T, T> valueMapping;

        public Setting(String key, List<String> aliasKeyNames, String description, Function<String, T> parser,
                       T defaultValue, boolean required, boolean encrypt) {
            this.key = key;
            this.aliasKeyNames = aliasKeyNames;
            this.description = description;
            this.defaultValue = defaultValue;
            this.hasDefaultValue = true;
            this.parser = parser;
            this.required = required;
            this.encrypt = encrypt;

            synchronized (registeredSettings) {
                Iterator<Setting<?>> it = registeredSettings.iterator();
                while (it.hasNext()) {
                    Setting setting = it.next();
                    if (setting.isKeyMatch(key)) {
                        LOG.warn("setting with key {} is already registered", key);
                        it.remove();
                        break;
                    }
                }
                registeredSettings.add(this);
            }
        }

        public Setting(String key, String description, Function<String, T> parser, T defaultValue, boolean required, boolean encrypt) {
            this(key, null, description, parser, defaultValue, required, encrypt);
        }

        public Setting(String key, String description, Function<String, T> parser) {
            this(key, null, description, parser, null, false, false);
            this.hasDefaultValue = false;
        }

        public Setting(String key, String description, Function<String, T> parser, T defaultValue) {
            this(key, null, description, parser, defaultValue, false, false);
        }

        public String getKey() {
            return key;
        }

        public T getDefaultValue() {
            return defaultValue;
        }

        private T getValue0(final Settings settings) {
            Object rawValue = getRawValue(settings);
            String stringValue = null;
            if (null != rawValue) {
                stringValue = rawValue.toString();
            }

            if (!StringUtils.isEmpty(stringValue)) {
                return parser.apply(stringValue);
            }
            if (hasDefaultValue) {
                return defaultValue;
            }
            if (!required) {
                return null;
            }

            throw new InvalidParameterException("key " + key + " not set in setting file");
        }

        public T getValue(final Settings settings) {
            T value = getValue0(settings);
            if (null != valueMapping) {
                if (null != value) {
                    value = valueMapping.getOrDefault(value, value);
                }
            }
            return value;
        }

        public void setValue(Settings settings, T value) {
            settings.set(key, value);
            if (null != aliasKeyNames) {
                for (String aliasName : aliasKeyNames) {
                    settings.set(aliasName, value);
                }
            }
        }

        public Object getRawValue(final Settings settings) {
            Object rs = null;
            if (null != settings) {
                rs = settings.internalGet(key);
                if (null == rs) {
                    if (null != aliasKeyNames) {
                        for (String aliasKeyName : aliasKeyNames) {
                            rs = settings.internalGet(aliasKeyName);
                            if (null != rs) {
                                break;
                            }
                        }
                    }
                }
            }
            return rs;
        }

        public String getRawValueWithEncryptFilter(final Settings settings) {
            if (this.encrypt) {
                return "******";
            }

            return String.valueOf(getValue(settings));
        }

        public Setting addValueMapping(T a, T b) {
            if (null == valueMapping) {
                valueMapping = new TreeMap<>();
            }
            valueMapping.put(a, b);
            return this;
        }

        public boolean isKeyMatch(String key) {
            if (StringUtils.equals(key, this.key)) {
                return true;
            }
            if (null != aliasKeyNames) {
                for (String aliasKeyName : aliasKeyNames) {
                    if (StringUtils.equals(key, aliasKeyName)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int compareTo(Setting o) {
            if (null == o) {
                return 1;
            }

            return key.compareTo(o.key);
        }

        public String toString(Settings settings) {
            StringBuilder sbl = new StringBuilder();

            sbl.append("Setting: [key=").append(key).append("] ");
            sbl.append("[value=").append(getRawValueWithEncryptFilter(settings)).append("]");

            return sbl.toString();
        }
    }
}
