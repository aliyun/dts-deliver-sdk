package com.aliyun.dts.deliver.commons.etl.impl;

import com.aliyun.dts.deliver.commons.etl.impl.WhiteList.DB_CATEGORY;
import com.aliyun.dts.deliver.commons.etl.impl.WhiteList.SCHEMA_MAPPER_MODE;
import com.aliyun.dts.deliver.commons.etl.impl.WhiteList.ItemType;
import com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior.DBNamesMapper;
import com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior.DBNamesMapperFactory;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.commons.util.Globs;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.iterators.ReverseListIterator;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RecursiveWhiteList implements WhiteList.WhiteListFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecursiveWhiteList.class);
    private static final EnumMap<DB_CATEGORY, List<String>> EXTRA_ATTRIBUTE_NAMES = new EnumMap<DB_CATEGORY, List<String>>(DB_CATEGORY.class) {
        {
            this.put(DB_CATEGORY.NORMAL, Arrays.asList(
                // drds
                "part_db_key", "part_tab_key", "tab_num_eachdb",
                // ads
                "primary_key", "cluster", "part_key", "part_num", "dbName",
                //odps
                "family", "partition",
                // tablestore
                "targetType"
            ));
            this.put(DB_CATEGORY.ELK, Arrays.asList(
                "analysis", "analyzer", "time_zone", "index_mapping", "is_partition", "partition_key", "_id",
                "_idvalue", "is_join", "relation_role", "parent_name", "parent_id", "index", "index_value"
            ));
        }
    };

    private Boolean sourceCaseInSensitive;
    private WhiteList.DB_CATEGORY srcDBCategory;
    private WhiteList.DB_CATEGORY destDBCategory;
    private WhiteListItem dbInstance;
    private KeywordHandler keywordHandler;
    private String destDbType;
    private DBNamesMapper dbNamesMapper;
    private int destLowerCaseTableNames;
    private String dbListCaseChangeMode;
    private SCHEMA_MAPPER_MODE schemaMapperMode;

    @Override
    public void initialize(String whitelistStr, WhiteList.DB_CATEGORY srcDBCategory,
                           WhiteList.DB_CATEGORY destDBCategory, boolean sourceCaseInSensitive, String destDbType, int destLowerCaseTableNames,
                           String dbListCaseChangeMode, String schemaMapperMode) throws Exception {
        this.sourceCaseInSensitive = sourceCaseInSensitive;

        this.srcDBCategory = srcDBCategory;
        this.destDBCategory = destDBCategory;
        this.dbListCaseChangeMode = dbListCaseChangeMode;
        this.schemaMapperMode = SCHEMA_MAPPER_MODE.parse(schemaMapperMode);

        this.keywordHandler = new KeywordHandler();

        if (!StringUtils.isEmpty(destDbType)) {
            this.destDbType = destDbType;
            this.destLowerCaseTableNames = destLowerCaseTableNames;
            this.dbNamesMapper = DBNamesMapperFactory.createDBNamesMapper(destDbType, 0, dbListCaseChangeMode);
        }

        recurseItems(null, "instance", WhiteList.ItemType.INSTANCE,
            parseJsonString(whitelistStr));
    }

    private Map<Object, Object> parseJsonString(String jsonString) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(jsonString, Map.class);
    }

    private String realGetItemName(String originItemName, ItemType itemType, WhiteListItem item, boolean isSource) {
        return realGetItemName(Pair.of(null, originItemName), itemType, item, isSource);
    }

    private String realGetItemName(Pair<String, String> namePair, ItemType itemType, WhiteListItem item, boolean isSource) {
        String originItemName = namePair.getRight();
        if (StringUtils.isEmpty(originItemName)) {
            return null;
        }

        if (null == item) {
            item = new WhiteListItem(itemType, "dummy");
            item.sourceSchema = namePair.getLeft();
            item.targetSchema = namePair.getLeft();
        }

        DB_CATEGORY dbCategory;
        String schemaName;
        if (isSource) {
            dbCategory = srcDBCategory;
            schemaName = item.sourceSchema;
        } else {
            dbCategory = destDBCategory;
            schemaName = item.targetSchema;
        }

        String itemName = curveName(originItemName, dbCategory, itemType);
        if (ItemType.TABLE == itemType) {
            if (!StringUtils.isEmpty(schemaName)) {
                itemName = processCurvedSchemaName(dbCategory,
                    curveName(schemaName, dbCategory, ItemType.SCHEMA),
                    itemName,
                    isSource);
            }
        }

        return itemName;
    }

    private String getSourceItemName(String originItemName, ItemType itemType, WhiteListItem item) {
        return realGetItemName(originItemName, itemType, item, true);
    }

    private Pair<String, String> extractRealSourceItemName(String sourceItemName) {
        if (DB_CATEGORY.MSSQL != srcDBCategory) {
            return Pair.of(null, sourceItemName);
        }

        int length = StringUtils.length(sourceItemName);
        if (length < 3) {
            return Pair.of(null, sourceItemName);
        }

        if (StringUtils.startsWith(sourceItemName, "[")
            && StringUtils.endsWithIgnoreCase(sourceItemName, "]")) {
            String realItemName = StringUtils.substring(sourceItemName, 1, length - 1);
            String[] splitItems = StringUtils.splitByWholeSeparator(realItemName, "].[");
            if (2 == splitItems.length) {
                return Pair.of(splitItems[0], splitItems[1]);
            }
            return Pair.of(null, realItemName);
        }

        return Pair.of(null, sourceItemName);
    }

    private String getDestItemName(String originItemName, ItemType itemType, WhiteListItem item) {
        if (null == item) {
            // missing exact mapping rule, which means the @originalItemName comes from source name
            return realGetItemName(extractRealSourceItemName(originItemName), itemType, null, false);
        }

        return realGetItemName(originItemName, itemType, item, false);
    }

    private WhiteListItem getDBInstanceItem() {
        WhiteListItem item = new WhiteListItem(ItemType.INSTANCE, "instance");
        item.filterType = FilterType.WHITE;
        item.includeAll = false;

        return item;
    }

    private void recurseItems(WhiteListItem parentItem, String itemName, WhiteList.ItemType itemType,
                              Map<Object, Object> itemAttributes) {

        WhiteListItem currentItem = null;

        // 1st: allocate current item
        if (null == parentItem) {
            // parent item is null, which is the root white list item
            dbInstance = getDBInstanceItem();
            currentItem = dbInstance;
        } else {
            String targetName = getOrDefaultNull(itemAttributes, "name");
            currentItem = new WhiteListItem(itemType, targetName);
        }

        // 2nd: extract attributes of current item
        currentItem.loadAttributesFromMap(itemAttributes);

        // 3rd: build the hierarchy
        itemName = getSourceItemName(itemName, itemType, currentItem);
        currentItem.sourceName = itemName;
        if (null != parentItem) {
            currentItem = parentItem.addSubItem(itemType, itemName, currentItem);
        }

        // 3rd: loop child items
        List<Pair<ItemType, Map<Object, Object>>> childItems = new ArrayList<>();
        if (currentItem == dbInstance) {
            // special case for the ill dblist definition
            childItems.add(Pair.of(ItemType.DATABASE, itemAttributes));
        } else {
            // good dblist definition
            for (Map.Entry<Object, Object> entry : itemAttributes.entrySet()) {
                String entryName = String.valueOf(entry.getKey());
                if (StringUtils.isEmpty(entryName)) {
                    continue;
                }
                WhiteList.ItemType subItemType = WhiteList.ItemType.parse(entryName);
                if (WhiteList.ItemType.NONE == subItemType) {
                    continue;
                }
                if (entry.getValue() instanceof Map) {
                    childItems.add(Pair.of(subItemType, (Map<Object, Object>) entry.getValue()));
                }
            }
        }
        for (Pair<ItemType, Map<Object, Object>> subItemPair : childItems) {
            Map<Object, Object> subItems = subItemPair.getRight();
            for (Map.Entry<Object, Object> subItemEntry : subItems.entrySet()) {
                if (subItemEntry.getValue() instanceof Map) {
                    String subItemEntryName = String.valueOf(subItemEntry.getKey());
                    recurseItems(currentItem, subItemEntryName, subItemPair.getLeft(), (Map<Object, Object>) subItemEntry.getValue());
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:EmptyBlock")
    private String curveName(String name, DB_CATEGORY category, ItemType itemType) {
        String curvedName = name;

        if (category.equals(DB_CATEGORY.MSSQL)) {
            if (StringUtils.startsWith(name, "[") && StringUtils.endsWith(name, "]")) {
                // do nothing
            } else {
                curvedName = "[" + name + "]";
            }
        } else if (category.equals(DB_CATEGORY.ELK)) {
            if (ItemType.TABLE == itemType) {
                String indexName = name;

                //转义
                indexName = indexName.replace("+", "_");
                indexName = indexName.replace("-", "_");
                indexName = indexName.replace("&&", "_");
                indexName = indexName.replace("||", "_");
                indexName = indexName.replace("!", "_");
                indexName = indexName.replace("()", "_");
                indexName = indexName.replace("{}", "_");
                indexName = indexName.replace("^", "_");
                indexName = indexName.replace("\"", "_");
                indexName = indexName.replace("~", "_");
                indexName = indexName.replace("*", "_");
                indexName = indexName.replace("?", "_");
                indexName = indexName.replace(":", "_");
                indexName = indexName.replace("\\", "_");
                indexName = indexName.replace("%", "_");

                curvedName = indexName;
            }
        }

        return curvedName;
    }

    private String processCurvedSchemaName(DB_CATEGORY dbCategory, String schemaName, String itemName, boolean isSource) {
        // schema mapper only work for dest db name
        if (!isSource) {
            switch (schemaMapperMode) {
                case DATABASE_SCHEMA:
                case WITHOUT_SCHEMA:
                    return itemName;
                default:
                    break;
            }
        }
        return schemaName + "." + itemName;
    }

    private <T> T realVisitDBInstance(String dbName, Pair<String, ItemType> tbPair, String columnName, T defaultValue,
                                      BiFunction<List<WhiteListItem>, WhiteListItem, Pair<T, Boolean>> visitor) {
        return realVisitDBInstance(dbName, tbPair, columnName, defaultValue, visitor, false, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED);
    }

    private <T> T realVisitDBInstance(String dbName, Pair<String, ItemType> tbPair, String columnName, T defaultValue,
                                      BiFunction<List<WhiteListItem>, WhiteListItem, Pair<T, Boolean>> visitor, boolean shouldProcessNullItem,
                                      NameMatchWay nameMatchWay, MatchRulePriority matchRulePriority) {
        return realVisitDBInstance(dbName, tbPair, columnName, defaultValue, visitor, shouldProcessNullItem, nameMatchWay, matchRulePriority, false);
    }

    private <T> T realVisitDBInstance(String dbName, Pair<String, ItemType> tbPair, String columnName, T defaultValue,
                                      BiFunction<List<WhiteListItem>, WhiteListItem, Pair<T, Boolean>> visitor, boolean shouldProcessNullItem,
                                      NameMatchWay nameMatchWay, MatchRulePriority matchRulePriority, boolean isRaw) {
        List<String> itemNames = Arrays.asList(dbName, tbPair.getLeft(), columnName);
        List<ItemType> itemTypes = Arrays.asList(ItemType.DATABASE, tbPair.getRight(), ItemType.COLUMN);
        List<WhiteListItem> parents = new ArrayList<>(3);
        WhiteListItem whiteListItem = null;
        T result = null;

        // we iterate by bfs, we need the stack to save the context
        Stack<Pair<WhiteListItem, Integer>> loopLevelStack = new Stack<>();

        for (int i = 0; i < itemNames.size(); i++) {
            String itemName = itemNames.get(i);
            ItemType itemType = itemTypes.get(i);

            Pair<T, Boolean> visitorResult = null;
            if (StringUtils.isEmpty(itemName)) {
                // check result
                visitorResult = visitor.apply(parents, whiteListItem);
                result = visitorResult.getLeft();
                if (visitorResult.getRight()) {
                    break;
                }
                if (loopLevelStack.empty()) {
                    break;
                }
                Pair<WhiteListItem, Integer> stackPair = loopLevelStack.pop();
                i = stackPair.getRight();
                whiteListItem = stackPair.getLeft();
                for (int j = i; j < parents.size(); j++) {
                    parents.remove(j);
                }
                continue;
            }

            if (null == whiteListItem) {
                whiteListItem = dbInstance;
            } else {
                // only add whitelist items since db level
                parents.add(whiteListItem);
            }

            List<WhiteListItem> matchedSubItems = whiteListItem.getMatchedSubItems(itemType, itemName, nameMatchWay, matchRulePriority, isRaw);

            // check we reach the last level. If so, we should call visitor
            if (matchedSubItems.isEmpty()) {
                if (shouldProcessNullItem) {
                    visitorResult = visitor.apply(parents, null);
                } else {
                    visitorResult = Pair.of(defaultValue, false);
                }
            } else if ((i + 1) == itemNames.size()) {
                for (WhiteListItem matchedSubItem : matchedSubItems) {
                    visitorResult = visitor.apply(parents, matchedSubItem);
                    if (visitorResult.getRight()) {
                        break;
                    }
                }
            }
            if (null != visitorResult) {
                result = visitorResult.getLeft();
                if (visitorResult.getRight()) {
                    break;
                }
                if (loopLevelStack.empty()) {
                    break;
                }
                Pair<WhiteListItem, Integer> stackPair = loopLevelStack.pop();
                i = stackPair.getRight();
                whiteListItem = stackPair.getLeft();
                for (int j = i; j < parents.size(); j++) {
                    parents.remove(j);
                }
                continue;
            } else {
                // start next loop
                ReverseListIterator<WhiteListItem> reverseIterator = new ReverseListIterator(matchedSubItems);
                while (reverseIterator.hasNext()) {
                    WhiteListItem matchedItem = reverseIterator.next();
                    loopLevelStack.push(Pair.of(matchedItem, i));
                }
                if (loopLevelStack.empty()) {
                    break;
                }
                Pair<WhiteListItem, Integer> stackPair = loopLevelStack.pop();
                i = stackPair.getRight();
                whiteListItem = stackPair.getLeft();
                for (int j = i; j < parents.size(); j++) {
                    parents.remove(j);
                }
                continue;
            }
        }

        return result;
    }

    private <T> T visitDBInstance(String dbName, String tbName, String columnName, T defaultValue,
                                  BiFunction<List<WhiteListItem>, WhiteListItem, Pair<T, Boolean>> visitor) {
        return realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), columnName, defaultValue, visitor);
    }

    private ItemType getItemType(String dbName, String tbName, String columnName) {
        if (StringUtils.isEmpty(dbName)) {
            return ItemType.INSTANCE;
        }
        if (StringUtils.isEmpty(tbName)) {
            return ItemType.DATABASE;
        }
        if (StringUtils.isEmpty(columnName)) {
            return ItemType.TABLE;
        }
        return ItemType.COLUMN;
    }

    private Pair<List<String>, Boolean> getItemGrayNames(List<WhiteListItem> items, List<String> candidateNames) {
        assert items.size() <= candidateNames.size();

        List<String> sourceNames = new ArrayList<>(3);
        Boolean hasGrayName = false;

        for (int i = 0; i < items.size(); i++) {
            WhiteListItem item = items.get(i);
            String candidateName = candidateNames.get(i);
            if (FilterType.GRAY == item.filterType) {
                sourceNames.add(item.getTargetGrayName(candidateName));
                hasGrayName = true;
            } else {
                sourceNames.add(candidateName);
            }
        }

        return Pair.of(sourceNames, hasGrayName);
    }

    private void append2StringBuffer(StringBuilder sbl, String value) {
        if (null == value) {
            return;
        }
        sbl.append(value);
    }

    @Override
    public String dbMapper(String dbName, String tbName, String columnName) {
        final ItemType itemType = getItemType(dbName, tbName, columnName);
        final Function<WhiteListItem, Boolean> composeDBName = (currentItem) -> {
            if (null != currentItem) {
                // just use the target name in dblist
                return false;
            }
            if (DB_CATEGORY.ELK != destDBCategory) {
                return false;
            }
            if (ItemType.TABLE != itemType) {
                // only compose table name
                return false;
            }
            String indexMapping = String.valueOf(getExtraAttribute(dbName, null, null, "index_mapping"));
            if (!StringUtils.equalsIgnoreCase("db_tb", indexMapping)) {
                return false;
            }
            return true;
        };
        final Supplier<String> getCandidateName = () -> {
            if (ItemType.DATABASE == itemType) {
                return (null == this.dbNamesMapper) ? dbName : this.dbNamesMapper.mapDatabaseName(dbName);
            }
            if (ItemType.TABLE == itemType) {
                return (null == this.dbNamesMapper) ? tbName : this.dbNamesMapper.mapTableName(tbName);
            }
            if (ItemType.COLUMN == itemType) {
                return (null == this.dbNamesMapper) ? columnName : this.dbNamesMapper.mapColumnName(columnName);
            }
            return null;
        };

        final String candidateName = getCandidateName.get();

        final BiFunction<List<WhiteListItem>, WhiteListItem, String> getItemName = (parentItems, item) -> {
            StringBuilder sbl = new StringBuilder();
            List<PlaceHolder> placeHolders = item.getTargetName(candidateName);

            for (PlaceHolder placeHolder : placeHolders) {
                if (placeHolder instanceof PlaceHolderValue) {
                    append2StringBuffer(sbl, ((PlaceHolderValue) placeHolder).value);
                } else if (placeHolder instanceof PlaceHolderKeyword) {
                    sbl.append(
                        keywordHandler.handleKeyword(
                            getItemGrayNames(parentItems, Arrays.asList(dbName, tbName, columnName)).getLeft(),
                            candidateName, item, ((PlaceHolderKeyword) placeHolder).keyword));
                } else {
                    throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_UNSUPPORTED_OPERATION, "not support place holder with type " + placeHolder.getClass().getName());
                }
            }
            return sbl.toString();
        };

        return realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), columnName, candidateName, (parentItems, item) -> {
            String name = null;
            if (null != item) {
                name = getItemName.apply(parentItems, item);
            }

            // always set name to be a not empty name
            if (StringUtils.isEmpty(name)) {
                name = candidateName;
            }
            if (composeDBName.apply(item)) {
                name = dbName + "_" + name;
            }

            return Pair.of(getDestItemName(name, itemType, item), true);
        }, true, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED);
    }

    @Override
    public String itemMapper(String dbName, String itemName, ItemType itemType) {
        return realVisitDBInstance(dbName, Pair.of(itemName, itemType), null, null,
            (parentItem, item) -> Pair.of(item.targetName, true));
    }

    @Override
    public Object getExtraAttribute(String dbName, String tbName, String columnName, String attributeName) {
        return visitDBInstance(dbName, tbName, columnName, null,
            (parentItem, item) -> Pair.of(item.getExtraAttribute(attributeName), true));
    }

    private Boolean realShouldIgnore(String dbName, String tbName, String columnName, NameMatchWay nameMatchWay) {

        BiFunction<List<String>, WhiteListItem, Boolean> recursiveShouldIgnore = (parentGrayNames, item) -> {
            assert null == item || item.filterType == FilterType.GRAY;

            String recursiveDBName = dbName;
            String recursiveTBName = tbName;
            String recursiveColumnName = columnName;
            NameMatchWay recursiveNameMatchWay = item == null ? nameMatchWay : item.grayNameMatchWay;

            switch (parentGrayNames.size()) {
                case 3:
                    recursiveColumnName = parentGrayNames.get(2);
                    item = null;
                    // pass through
                case 2:
                    if (null != item) {
                        recursiveColumnName = item.getTargetGrayName(columnName);
                        item = null;
                    }
                    recursiveTBName = parentGrayNames.get(1);
                    // pass through
                case 1:
                    if (null != item) {
                        recursiveTBName = item.getTargetGrayName(tbName);
                        item = null;
                    }
                    recursiveDBName = parentGrayNames.get(0);
                case 0:
                    if (null != item) {
                        recursiveDBName = item.getTargetGrayName(dbName);
                    }
            }

            if (StringUtils.equals(recursiveDBName, dbName)
                && StringUtils.equals(recursiveTBName, tbName)
                && StringUtils.equals(recursiveColumnName, columnName)) {
                throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "gray item has same source and gray names");
            }

            return realShouldIgnore(recursiveDBName, recursiveTBName, recursiveColumnName, recursiveNameMatchWay);
        };

        return realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), columnName, Boolean.TRUE, (parentItems, item) -> {

            boolean shouldRecursive = false;
            Pair<List<String>, Boolean> parentNamesPair = getItemGrayNames(parentItems, Arrays.asList(dbName, tbName, columnName));
            if (parentNamesPair.getRight()) {
                // there is parent with gray name, so we should do recursive loop
                shouldRecursive = true;
            }

            if (null != item) {
                // exact match, just check the filter type black or white
                switch (item.filterType) {
                    case BLACK:
                        if (item.includeAll) {
                            return Pair.of(true, true);
                        } else {
                            return Pair.of(true, false);
                        }
                    case GRAY:
                        shouldRecursive = true;
                        break;
                    case WHITE:
                        // pass through
                    default:
                        return Pair.of(false, true);
                }
            }

            if (shouldRecursive) {
                // gray name in parent or current item, we should do recursive by gray names
                return Pair.of(recursiveShouldIgnore.apply(parentNamesPair.getLeft(), item), true);
            }

            WhiteListItem parentItem = parentItems.size() > 0 ? parentItems.get(parentItems.size() - 1) : null;
            if (null != parentItem) {
                parentItems.remove(parentItems.size() - 1);

                switch (parentItem.filterType) {
                    case BLACK:
                        // parent is in blacklist, so the children must be in it
                        if (parentItem.includeAll) {
                            return Pair.of(true, true);
                        } else {
                            return Pair.of(true, false);
                        }
                    case GRAY:
                        new InvalidParameterException("this should never happen");
                    case WHITE:
                        // parentItem is white, just check include flag
                        // pass through
                    default:
                        if (parentItem.includeAll) {
                            return Pair.of(false, true);
                        } else {
                            return Pair.of(true, false);
                        }
                }
            }
            return Pair.of(true, true);
        }, true, nameMatchWay, MatchRulePriority.BLACK_FIRST);
    }

    @Override
    public Boolean shouldIgnore(String dbName, String tbName, String columnName) {
        return realShouldIgnore(dbName, tbName, columnName, NameMatchWay.EXACT);
    }

    @Override
    public Set<String> allDatabases() {
        return dbInstance.getSubItems(ItemType.DATABASE).values().stream()
            .map(item -> item.sourceName)
            .collect(Collectors.toSet());
    }

    @Override
    public void addWhiteListItem(String json) throws Exception {
        Stack<Pair<WhiteListItem, WhiteListItem>> pendingItems = new Stack<>();

        final BiFunction<WhiteListItem, WhiteListItem, WhiteListItem> findSubItemWithSameSourceName = (parentItem, sourceItem) -> {
            WhiteListItem targetItem = null;
            switch (sourceItem.patternType) {
                case BARE_NAME:
                    targetItem = parentItem.subItemsByType.getOrDefault(sourceItem.itemType, Collections.emptyMap())
                        .get(sourceItem.sourceName);
                    break;
                case REGEX:
                case FNMATCH:
                    targetItem = parentItem.subPatternItemsByType.getOrDefault(sourceItem.itemType, Collections.emptyList())
                        .stream()
                        .filter(item -> StringUtils.equals(sourceItem.sourceName, item.sourceName))
                        .findAny()
                        .orElse(null);
                    break;
                default:
                    targetItem = null;
            }
            return targetItem;
        };

        final BiConsumer<WhiteListItem, WhiteListItem> mergeItem = (sourceItem, targetItem) -> {
            if (null != sourceItem.targetName && null != targetItem.targetName
                && !StringUtils.equals(sourceItem.targetName, targetItem.targetName)) {
                throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_UNSUPPORTED_OPERATION, "do not support merge white list item with different target name");
            }
            if (sourceItem.includeAll) {
                targetItem.includeAll = true;
            }
        };

        final BiConsumer<WhiteListItem, Collection<? extends WhiteListItem>> pushSubItems = (parentItem, subItems) -> {
            for (WhiteListItem subItem : subItems) {
                pendingItems.push(Pair.of(subItem, parentItem));
            }
        };

        RecursiveWhiteList tmpWhiteList = new RecursiveWhiteList();
        tmpWhiteList.initialize(json, srcDBCategory, destDBCategory, sourceCaseInSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode.name());
        WhiteListItem tmpDBInstance = tmpWhiteList.dbInstance;

        // merge tmpDBInstance to dbInstance
        pendingItems.push(Pair.of(tmpDBInstance, null));
        while (!pendingItems.isEmpty()) {

            Pair<WhiteListItem, WhiteListItem> itemPair = pendingItems.pop();
            WhiteListItem itemToBeMerged = itemPair.getLeft();
            WhiteListItem targetParentItem = itemPair.getRight();

            WhiteListItem targetItem = null;

            // try find item in target tree
            if (ItemType.INSTANCE == itemToBeMerged.itemType) {
                targetItem = dbInstance;
            } else {
                targetItem = findSubItemWithSameSourceName.apply(targetParentItem, itemToBeMerged);
            }

            if (null == targetItem) {
                targetParentItem.addSubItem(itemToBeMerged.itemType, itemToBeMerged.sourceName, itemToBeMerged);
            } else {
                mergeItem.accept(itemToBeMerged, targetItem);

                // pub sub items to pending stack, that will be processed next loop
                for (Map<String, WhiteListItem> subItems : itemToBeMerged.subItemsByType.values()) {
                    pushSubItems.accept(targetItem, subItems.values());
                }
                for (List<PatternItem> patternItems : itemToBeMerged.subPatternItemsByType.values()) {
                    pushSubItems.accept(targetItem, patternItems);
                }
            }
        }
    }

    private void dfsWhiteListItem(WhiteListItem parentItem, WhiteListItem item,
                                  BiFunction<WhiteListItem, WhiteListItem, Boolean> itemConsumer) {
        if (itemConsumer.apply(parentItem, item)) {
            for (Map<String, WhiteListItem> itemsOfSameType : item.subItemsByType.values()) {
                for (WhiteListItem subItem : itemsOfSameType.values()) {
                    dfsWhiteListItem(item, subItem, itemConsumer);
                }
            }
            for (List<PatternItem> itemsOfSameType : item.subPatternItemsByType.values()) {
                for (PatternItem subItem : itemsOfSameType) {
                    dfsWhiteListItem(item, subItem, itemConsumer);
                }
            }
        }
    }

    @Override
    public List<Pair<String, String>> getAllScriptInfo() {
        List<Pair<String, String>> rs = new ArrayList<>(3);

        BiConsumer<WhiteListItem, WhiteListItem> extractScriptInfo = (parentItem, item) -> {
            if (!StringUtils.isEmpty(item.scriptName)) {
                rs.add(Pair.of(getScriptNamespace(parentItem, item), item.scriptName));
            }
        };

        dfsWhiteListItem(null, dbInstance, (parentItem, item) -> {
            switch (item.itemType) {
                case INSTANCE:
                    return true;
                case DATABASE:
                    extractScriptInfo.accept(parentItem, item);
                    return true;
                case TABLE:
                    extractScriptInfo.accept(parentItem, item);
                    // fall through
                default:
                    return false;
            }
        });

        return rs;
    }

    private String getScriptNamespace(WhiteListItem parentItem, WhiteListItem item) {
        String namespace = item.scriptNamespace;
        if (StringUtils.isEmpty(namespace)) {
            if (ItemType.DATABASE == item.itemType) {
                namespace = item.sourceName;
            } else {
                namespace = parentItem.sourceName;
            }
        }
        return namespace;
    }

    @Override
    public Optional<Pair<String, String>> getScriptInfo(String dbName, String tbName) {
        BiFunction<List<WhiteListItem>, Integer, WhiteListItem> getItem = (items, index) -> {
            if (index > -1) {
                return items.get(index);
            }

            return null;
        };

        return (Optional<Pair<String, String>>) realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), null, Optional.empty(),
            (parentItems, item) -> {
                WhiteListItem parentItem = getItem.apply(parentItems, parentItems.size() - 1);

                if (null == item) {
                    item = parentItem;
                    parentItem = getItem.apply(parentItems, parentItems.size() - 2);
                }
                if (null == item || StringUtils.isEmpty(item.scriptName)) {
                    return Pair.of(Optional.empty(), true);
                }

                return Pair.of(Optional.of(Pair.of(getScriptNamespace(parentItem, item), item.scriptName)), true);
            }, true, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED);
    }

    @Override
    public Pair<String, String> getScriptContent(String namespace, String scriptName) {
        return realVisitDBInstance(namespace, Pair.of(scriptName, ItemType.SCRIPTS), null, null,
            (parentItem, item) -> Pair.of(Pair.of(item.scriptType, item.scriptContent), true));
    }

    @Override
    public String getColumnValue(String dbName, String tbName, String columnName) {
        String columnValue = realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), columnName, null, (parentItems, item) -> {
            if (item == null) {
                return Pair.of(null, true);
            } else {
                return Pair.of(item.value, true);
            }
        }, true, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED);
        return columnValue;
    }

    public PatternType getPatternType(String dbName, String tbName) {

        PatternType patternType = realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), null, PatternType.BARE_NAME,
            (parentItems, item) -> {

                if (null == item || item.patternType == null) {
                    return Pair.of(PatternType.BARE_NAME, true);
                }

                return Pair.of(item.patternType, true);
            }, true, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED);

        return patternType;
    }

    @Override
    public PatternType getRawPatternType(String dbName, String tbName) {

        PatternType patternType = ((PatternType) realVisitDBInstance(dbName, Pair.of(tbName, ItemType.TABLE), null, Optional.empty(),
            (parentItems, item) -> {
                if (null == item || item.patternType == null) {
                    return Pair.of(PatternType.BARE_NAME, true);
                }

                return Pair.of(item.patternType, true);
            }, true, NameMatchWay.EXACT, MatchRulePriority.BLACK_EXCLUDED, true));

        return patternType;
    }

    public enum PatternType {
        BARE_NAME,
        REGEX,
        FNMATCH;

        static PatternType parse(String patternTypeString) {
            if (StringUtils.equalsIgnoreCase(PatternType.REGEX.name(), patternTypeString)) {
                return REGEX;
            } else if (StringUtils.equalsIgnoreCase(PatternType.FNMATCH.name(), patternTypeString)) {
                return FNMATCH;
            } else {
                return BARE_NAME;
            }
        }
    }

    enum FilterType {
        WHITE,
        BLACK,
        GRAY;

        static FilterType parse(String filterTypeString) {
            if (StringUtils.equalsIgnoreCase(FilterType.BLACK.name(), filterTypeString)) {
                return BLACK;
            } else if (StringUtils.equalsIgnoreCase(FilterType.GRAY.name(), filterTypeString)) {
                return GRAY;
            }

            return WHITE;
        }
    }

    enum MatchRulePriority {
        BLACK_FIRST,
        BLACK_EXCLUDED
    }

    enum NameMatchWay {
        EXACT,
        PREFIX;

        static NameMatchWay parse(String grayNameMatchWay) {
            if (StringUtils.equalsIgnoreCase(NameMatchWay.PREFIX.name(), grayNameMatchWay)) {
                return PREFIX;
            }
            return EXACT;
        }
    }

    private Map<String, WhiteListItem> getSubWhiteItemsMap() {
        if (sourceCaseInSensitive) {
            return new CaseInsensitiveMap();
        }

        return new TreeMap<>();
    }

    class BaseWhiteListItem {
        ItemType itemType;

        String sourceSchema;
        String targetSchema;
        String sourceName;
        String targetName;
        String targetGrayName;
        NameMatchWay grayNameMatchWay;
        String tableGroup;
        String messageKey;
        String conflictRule;
        String supportedDMLOperations;
        String supportedDDLOperations;

        String metaType;
        String defaultValue;

        Integer shardNumber = 5;
        Integer replicaNumber = 1;

        Boolean includeAll = true;
        Boolean caseSensitive;
        Boolean isSharedKey;

        Object dataFilter;

        PatternType patternType;
        FilterType filterType;

        String scriptNamespace;
        String scriptName;
        String scriptLocation;
        String scriptType;
        String scriptContent;
        String value;
    }

    private String getOrDefaultNull(Map<Object, Object> dataMap, String key) {
        return (String) getPrimitiveValueOrDefault(dataMap, key, null);
    }

    private Object getPrimitiveValueOrDefault(Map<Object, Object> dataMap, String key, Object defaultValue) {
        Object value = dataMap.getOrDefault(key, null);
        if (null == value) {
            return defaultValue;
        }

        if (value instanceof Map || value instanceof List) {
            return defaultValue;
        }

        return value;
    }

    class PlaceHolder {
    }

    class PlaceHolderPosition extends PlaceHolder {
        int position;

        PlaceHolderPosition(int position) {
            this.position = position;
        }
    }

    class PlaceHolderValue extends PlaceHolder {
        String value;

        PlaceHolderValue(String value) {
            this.value = value;
        }
    }

    class PlaceHolderKeyword extends PlaceHolder {
        String keyword;

        PlaceHolderKeyword(String keyword) {
            this.keyword = keyword;
        }
    }

    class WhiteListItem extends BaseWhiteListItem {

        EnumMap<WhiteList.ItemType, Map<String, WhiteListItem>> subItemsByType = new EnumMap<>(ItemType.class);
        EnumMap<WhiteList.ItemType, List<PatternItem>> subPatternItemsByType = new EnumMap<>(ItemType.class);

        Map<Object, Object> extraAttributes;

        WhiteListItem(ItemType itemType, String targetName) {
            this.itemType = itemType;
            this.targetName = targetName;
        }

        WhiteListItem addSubItem(WhiteList.ItemType itemType, String name, WhiteListItem subItem) {
            if (PatternType.BARE_NAME == subItem.patternType) {
                Map<String, WhiteListItem> subItems = subItemsByType.computeIfAbsent(itemType, (type) -> getSubWhiteItemsMap());
                subItems.put(name, subItem);
            } else {
                PatternItem patternSubItem = new PatternItem(itemType, name, subItem);
                List<PatternItem> patternSubItems = subPatternItemsByType.computeIfAbsent(itemType, (type) -> new ArrayList<>());
                if (FilterType.BLACK == patternSubItem.filterType) {
                    // add to head
                    patternSubItems.add(0, patternSubItem);
                } else {
                    patternSubItems.add(patternSubItem);
                }
                subItem = patternSubItem;
            }
            return subItem;
        }

        private WhiteListItem getSubItemFromBareName(Map<String, WhiteListItem> bareNameSubItems, String name, NameMatchWay nameMatchWay) {
            switch (nameMatchWay) {
                case EXACT:
                    return bareNameSubItems.get(name);
                default:
                    return bareNameSubItems.entrySet().stream()
                        .filter(entry -> StringUtils.startsWith(entry.getKey(), name))
                        .map(Map.Entry::getValue)
                        .findAny()
                        .orElse(null);
            }
        }

        List<WhiteListItem> getMatchedSubItems(ItemType subItemType, String name, NameMatchWay nameMatchWay, MatchRulePriority matchRulePriority, boolean isRaw) {
            List<WhiteListItem> matchedItems = new ArrayList<>(3);
            WhiteListItem item = getSubItemFromBareName(subItemsByType.getOrDefault(subItemType, Collections.emptyMap()),
                name, nameMatchWay);
            if (null != item) {
                matchedItems.add(item);
            }

            List<PatternItem> patternItems = subPatternItemsByType.getOrDefault(subItemType, Collections.emptyList());

            for (PatternItem patternItem : patternItems) {
                if (FilterType.BLACK == patternItem.filterType && MatchRulePriority.BLACK_EXCLUDED == matchRulePriority) {
                    continue;
                }
                if (patternItem.isMatch(name, isRaw)) {
                    matchedItems.add(patternItem);
                }
            }

            return matchedItems;
        }

        Map<String, WhiteListItem> getSubItems(ItemType subItemType) {
            return subItemsByType.getOrDefault(subItemType, Collections.emptyMap());
        }

        String getExtraAttribute(String key) {
            return getOrDefaultNull(extraAttributes, key);
        }

        List<PlaceHolder> getTargetName(String candidateSourceName) {
            return Arrays.asList(new PlaceHolderValue(targetName));
        }

        String getTargetGrayName(String candidateSourceName) {
            return targetGrayName;
        }

        private void loadExtraAttributes(Map<Object, Object> attributeMap) {
            extraAttributes = new TreeMap<>();
            EXTRA_ATTRIBUTE_NAMES.getOrDefault(destDBCategory, Collections.emptyList()).stream()
                .forEach(attributeName -> {
                    Object value = getPrimitiveValueOrDefault(attributeMap, attributeName, null);
                    if (null != value) {
                        extraAttributes.put(attributeName, value);
                    }
                });
        }

        void loadAttributesFromMap(Map<Object, Object> attributeMap) {

            this.targetGrayName = (String) getPrimitiveValueOrDefault(attributeMap, "grayName", sourceName);
            this.grayNameMatchWay = NameMatchWay.parse(getOrDefaultNull(attributeMap, "grayNameMatchWay"));

            this.sourceSchema = getOrDefaultNull(attributeMap, "srcSchema");
            this.targetSchema = getOrDefaultNull(attributeMap, "destSchema");

            this.tableGroup = getOrDefaultNull(attributeMap, "tableGroup");

            this.messageKey = getOrDefaultNull(attributeMap, "message_key");
            this.supportedDMLOperations = getOrDefaultNull(attributeMap, "dml_op");
            this.supportedDDLOperations = getOrDefaultNull(attributeMap, "ddl_op");
            this.conflictRule = getOrDefaultNull(attributeMap, "conflict");

            this.metaType = getOrDefaultNull(attributeMap, "type");
            this.defaultValue = getOrDefaultNull(attributeMap, "defaultValue");

            this.shardNumber = (Integer) getPrimitiveValueOrDefault(attributeMap, "shard", this.shardNumber);
            this.replicaNumber = (Integer) getPrimitiveValueOrDefault(attributeMap, "replica", this.replicaNumber);

            this.includeAll = (Boolean) getPrimitiveValueOrDefault(attributeMap, "all", this.includeAll);
            this.caseSensitive = (Boolean) getPrimitiveValueOrDefault(attributeMap, "case_sen", Boolean.FALSE);
            this.isSharedKey = (Boolean) getPrimitiveValueOrDefault(attributeMap, "sharedKey", Boolean.FALSE);

            this.dataFilter = getOrDefaultNull(attributeMap, "filter");

            this.patternType = PatternType.parse(getOrDefaultNull(attributeMap, "patternType"));
            this.filterType = FilterType.parse(getOrDefaultNull(attributeMap, "filterType"));

            this.scriptName = getOrDefaultNull(attributeMap, "scriptName");
            this.scriptNamespace = getOrDefaultNull(attributeMap, "scriptNamespace");
            this.scriptLocation = getOrDefaultNull(attributeMap, "scriptLocation");
            this.scriptType = getOrDefaultNull(attributeMap, "scriptType");
            this.scriptContent = getOrDefaultNull(attributeMap, "scriptContent");

            this.value = getOrDefaultNull(attributeMap, "value");

            loadExtraAttributes(attributeMap);
        }
    }

    class PatternItem extends WhiteListItem {

        Pattern sourcePattern;
        private ThreadLocal<Triple<String, Matcher, Boolean>> lastMatcher;
        private List<PlaceHolder> targetObjects;

        private List<PlaceHolder> targetGrayObjects;

        PatternItem(ItemType itemType, String sourceName, WhiteListItem whiteListItem) {
            super(itemType, whiteListItem.targetName);

            lastMatcher = new ThreadLocal<>();

            this.sourceName = sourceName;
            this.sourcePattern = compilePattern(sourceName, whiteListItem.patternType);

            sourceSchema = whiteListItem.sourceSchema;
            targetSchema = whiteListItem.targetSchema;
            targetName = whiteListItem.targetName;
            targetGrayName = whiteListItem.targetGrayName;
            grayNameMatchWay = whiteListItem.grayNameMatchWay;
            tableGroup = whiteListItem.tableGroup;
            messageKey = whiteListItem.messageKey;
            conflictRule = whiteListItem.conflictRule;
            supportedDMLOperations = whiteListItem.supportedDMLOperations;
            supportedDDLOperations = whiteListItem.supportedDDLOperations;
            metaType = whiteListItem.metaType;
            defaultValue = whiteListItem.defaultValue;
            shardNumber = whiteListItem.shardNumber;
            replicaNumber = whiteListItem.replicaNumber;
            includeAll = whiteListItem.includeAll;
            caseSensitive = whiteListItem.caseSensitive;
            isSharedKey = whiteListItem.isSharedKey;
            dataFilter = whiteListItem.dataFilter;
            patternType = whiteListItem.patternType;
            filterType = whiteListItem.filterType;
            subItemsByType = whiteListItem.subItemsByType;
            subPatternItemsByType = whiteListItem.subPatternItemsByType;
            extraAttributes = whiteListItem.extraAttributes;
        }

        private Pattern compilePattern(String patternString, PatternType patternType) {
            if (PatternType.FNMATCH == patternType) {
                patternString = Globs.toUnixRegexPattern(patternString);
            }
            return Pattern.compile(patternString);
        }

        private Pair<Matcher, Boolean> tryUseLastMatcher(String candidateSourceName) {
            Matcher matcher = null;
            Boolean isMatch = false;

            Triple<String, Matcher, Boolean> lastMatcherLocal = lastMatcher.get();

            if (null != lastMatcherLocal) {
                if (StringUtils.equals(lastMatcherLocal.getLeft(), candidateSourceName)) {
                    matcher = lastMatcherLocal.getMiddle();
                    isMatch = lastMatcherLocal.getRight();
                }
            }
            if (null == matcher) {
                matcher = sourcePattern.matcher(candidateSourceName);
                isMatch = matcher.matches();
                lastMatcherLocal = Triple.of(candidateSourceName, matcher, isMatch);
            }

            lastMatcher.set(lastMatcherLocal);

            return Pair.of(matcher, isMatch);
        }

        boolean isMatch(String candidateSourceName) {
            return isMatch(candidateSourceName, false);
        }

        boolean isMatch(String candidateSourceName, boolean isRaw) {
            if (isRaw) {
                return StringUtils.equals(candidateSourceName, sourceName);
            } else {
                Pair<Matcher, Boolean> matcher = tryUseLastMatcher(candidateSourceName);
                return matcher.getRight();
            }
        }

        private List<PlaceHolder> realParseTargetName(String targetName) {
            List<PlaceHolder> rs = new ArrayList<>();

            if (StringUtils.isEmpty(targetName)) {
                return rs;
            }

            boolean braceBegin = false;
            StringBuilder sbl = new StringBuilder();
            for (int i = 0; i < targetName.length(); i++) {
                char charValue = targetName.charAt(i);
                if (charValue == '{') {
                    braceBegin = true;
                    if (sbl.length() > 0) {
                        rs.add(new PlaceHolderValue(sbl.toString()));
                    }
                    sbl = new StringBuilder();
                } else if (charValue == '}') {
                    if (braceBegin) {
                        if (sbl.length() > 0) {
                            try {
                                int position = Integer.parseInt(sbl.toString());
                                rs.add(new PlaceHolderPosition(position));
                            } catch (Exception foo) {
                                rs.add(new PlaceHolderKeyword(sbl.toString()));
                            }
                        }
                    }
                    sbl = new StringBuilder();
                } else {
                    sbl.append(charValue);
                }
            }
            if (sbl.length() > 0) {
                rs.add(new PlaceHolderValue(sbl.toString()));
            }

            return rs;
        }

        private synchronized void parseTargetNameIfNeeded() {
            if (null == targetObjects) {
                targetObjects = realParseTargetName(targetName);
            }
            if (null == targetGrayObjects) {
                targetGrayObjects = realParseTargetName(targetGrayName);
            }
        }

        private List<PlaceHolder> realGetTargetName(String candidateSourceName, List<PlaceHolder> targetPlaceHolders) {
            StringBuilder sbl = new StringBuilder();
            Pair<Matcher, Boolean> matcherPair = tryUseLastMatcher(candidateSourceName);
            Matcher matcher = matcherPair.getLeft();

            List<PlaceHolder> rs = new ArrayList<>(3);

            String keyword = null;

            if (!matcherPair.getRight()) {
                return null;
            }

            for (PlaceHolder holder : targetPlaceHolders) {
                if (holder instanceof PlaceHolderValue) {
                    append2StringBuffer(sbl, ((PlaceHolderValue) holder).value);
                } else if (holder instanceof PlaceHolderPosition) {
                    int pos = ((PlaceHolderPosition) holder).position;
                    if (pos <= matcher.groupCount()) {
                        sbl.append(matcher.group(pos));
                    }
                } else if (holder instanceof PlaceHolderKeyword) {
                    // only support a keyword place holder
                    if (null != keyword) {
                        throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "only support one keyword in target name");
                    }

                    if (sbl.length() > 0) {
                        rs.add(new PlaceHolderValue(sbl.toString()));
                        sbl = new StringBuilder();
                    }

                    rs.add(holder);
                }
            }

            if (sbl.length() > 0) {
                rs.add(new PlaceHolderValue(sbl.toString()));
            }

            return rs;
        }

        @Override
        List<PlaceHolder> getTargetName(String candidateSourceName) {
            parseTargetNameIfNeeded();
            return realGetTargetName(candidateSourceName, targetObjects);
        }

        @Override
        String getTargetGrayName(String candidateSourceName) {
            parseTargetNameIfNeeded();

            StringBuilder sbl = new StringBuilder();
            List<PlaceHolder> placeHolders = realGetTargetName(candidateSourceName, targetGrayObjects);
            for (PlaceHolder placeHolder : placeHolders) {
                if (placeHolder instanceof PlaceHolderValue) {
                    sbl.append(((PlaceHolderValue) placeHolder).value);
                } else {
                    throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "not support gray name with place holder " + placeHolder.getClass().getName());
                }
            }

            return sbl.toString();
        }
    }

    class KeywordHandler {

        String handleKeyword(List<String> parentSourceNames, String candidateName, WhiteListItem item, String keyword) {

            if (StringUtils.equalsIgnoreCase(keyword, "grayNameMapped")) {
                String grayName = item.getTargetGrayName(candidateName);
                parentSourceNames.add(grayName);
                while (parentSourceNames.size() < 3) {
                    parentSourceNames.add(null);
                }

                return RecursiveWhiteList.this.dbMapper(parentSourceNames.get(0), parentSourceNames.get(1), parentSourceNames.get(2));
            }

            return keyword;
        }
    }
}
