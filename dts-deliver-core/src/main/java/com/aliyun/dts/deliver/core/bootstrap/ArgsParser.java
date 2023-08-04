package com.aliyun.dts.deliver.core.bootstrap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class ArgsParser {

    private static Map<String, ArgSettingInfo> argSettingMap = new HashMap<>();

    private static final OptionParser OPTION_PARSER = new OptionParser();

    private static final OptionSpec<String> SETTINGS_OPTION =
            OPTION_PARSER.accepts("settings")
                    .withRequiredArg();

    private static final OptionSpec<String> CONFIG_PATH_OPTION = OPTION_PARSER
        .accepts("config", "job config file path")
        .withRequiredArg()
        .defaultsTo("./config");

    private final Map<String, String> settingValueMap;
    private OptionSet optionSet;

    ArgsParser(OptionSet optionSet) {
        this.optionSet = optionSet;
        settingValueMap = getSettingsFromArgsParser();
    }

    public static ArgsParser parse(String[] args) {
        OPTION_PARSER.allowsUnrecognizedOptions();
        OPTION_PARSER.posixlyCorrect(false);
        OptionSet optionSet = OPTION_PARSER.parse(args);
        return new ArgsParser(optionSet);
    }

    public String getConfigPath() {
        return CONFIG_PATH_OPTION.value(optionSet);
    }

    public String getMode() {
        for (Object obj : optionSet.nonOptionArguments()) {
            String arg = String.valueOf(obj);
            if (StringUtils.startsWithIgnoreCase(arg, "-mode")) {
                String[] ret = arg.split("=");
                if (ret.length == 2) {
                    return ret[1];
                }
            }
        }
        return "unknown";
    }

    public Map<String, String> getSettingValueMap() {
        return settingValueMap;
    }

    private void parseAndAddSettings(String settings, Map<String, String> output) {
        final String settingSeparator = ";";
        final String keyValueSeparator = "=";

        int settingStartPos = 0;
        int settingEndPos = 1;

        while (settingEndPos > 0) {
            settingEndPos = StringUtils.indexOf(settings, settingSeparator, settingStartPos);

            int currentStartPos = settingStartPos;
            settingStartPos = settingEndPos + 1;
            int currentEndPos = settingEndPos > 0 ? settingEndPos : StringUtils.length(settings);

            int keyValueSeparatorPos = StringUtils.indexOf(settings, keyValueSeparator, currentStartPos);
            if (keyValueSeparatorPos < 0) {
                continue;
            }
            if (keyValueSeparatorPos > currentEndPos) {
                continue;
            }

            String key = StringUtils.substring(settings, currentStartPos, keyValueSeparatorPos);
            String value = StringUtils.substring(settings, keyValueSeparatorPos + 1, currentEndPos);
            output.put(key, value);
        }
    }

    private Map<String, String> getSettingsFromArgsParser() {
        Map<String, String> valueMap = new HashMap<>();
        for (Map.Entry<String, ArgSettingInfo> entry : argSettingMap.entrySet()) {
            ArgSettingInfo argSettingInfo = entry.getValue();

            if (optionSet.has(argSettingInfo.optionName)) {
                Object value = optionSet.valueOf(argSettingInfo.optionName);
                if (!argSettingInfo.hasArg) {
                    value = "true";
                }
                valueMap.put(entry.getKey(), String.valueOf(value));
            }
        }

        if (optionSet.has(SETTINGS_OPTION)) {
            parseAndAddSettings(optionSet.valueOf(SETTINGS_OPTION), valueMap);
        }

        return valueMap;
    }

    public static OptionSpecBuilder mergeSetting(String key, String option, boolean hasArg) {
        ArgSettingInfo argSettingInfo = new ArgSettingInfo();
        argSettingInfo.optionName = option;
        argSettingInfo.hasArg = hasArg;

        argSettingMap.put(key, argSettingInfo);

        return OPTION_PARSER.accepts(option);
    }

    static class ArgSettingInfo {
        String optionName;
        boolean hasArg;
    }
}
