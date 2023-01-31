package org.apache.hudi.utilities.sources.processor.canal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;
import org.apache.spark.api.java.JavaRDD;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.hudi.utilities.sources.processor.canal.PreCombineFieldType.DATE_STRING;
import static org.apache.hudi.utilities.sources.processor.canal.PreCombineFieldType.valueOf;
import static org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.*;

public class CanalJsonKafkaSourcePostProcessor extends JsonKafkaSourcePostProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Option<String> databaseRegex;
    private final String tableRegex;


    public CanalJsonKafkaSourcePostProcessor(TypedProperties props) {
        super(props);
        databaseRegex = Option.ofNullable(props.getString(Config.DATABASE_NAME_REGEX_PROP.key()));
        tableRegex = props.getString(Config.TABLE_NAME_REGEX_PROP.key());
    }


    /**
     * Partial fields in canal json string
     * @param inputJsonRecords
     * @return
     */
    private static final String destination = "destination";// 对应canal的实例或者MQ的topic
    private static final String groupId = "groupId"; // 对应mq的group id
    private static final String DATABASE = "database";// 数据库或schema
    private static final String TABLE = "table";   // 表名
    private static final String PK_NAMES = "pkNames";
    private static final String IS_DDL = "isDdl";
    private static final String TYPE = "type";   // 类型: INSERT UPDATE DELETE
    // binlog executeTime
    private static final String ES = "es"; // 执行耗时
    // dml build timeStamp
    private static final String TS = "ts"; // 同步时间
    private static final String SQL = "sql";   // 执行的sql, dml sql为空
    private static final String DATA = "data"; // 数据列表
    private static final String OLD = "old";   // 旧数据列表, 用于update, size和data的size一一对应

    // ------------------------------------------------------------------------
    //  Operation types
    // ------------------------------------------------------------------------

    private static final String INSERT = "insert";
    private static final String UPDATE = "update";
    private static final String DELETE = "delete";


    @Override
    public JavaRDD<String> process(JavaRDD<String> inputJsonRecords) {

        return inputJsonRecords.map(record -> {
            JsonNode inputJson = MAPPER.readTree(record);
            String databaseName = inputJson.get(DATABASE).textValue();
            String tableName = inputJson.get(TABLE).textValue();

            //filter out target databases and tables
            if (isTargetTable(databaseName, tableName)) {
                ObjectNode result = (ObjectNode) inputJson.get(DATA);
                String type = inputJson.get(TYPE).textValue();

                // insert or update
                if (INSERT.equals(type) || UPDATE.equals(type)) {
                    // tag this record not delete.
                    result.put(HoodieRecord.HOODIE_IS_DELETED, false);
                    return result.toString();

                    // delete
                } else if (DELETE.equals(type)) {
                    return processDelete(inputJson, result);
                } else {
                    // there might be some ddl data, ignore it
                    return null;
                }
            } else {
                // not the data from target table(s), ignore it
                return null;
            }
        }).filter(Objects::nonNull);
    }


    private String processDelete(JsonNode inputJson, ObjectNode result) {
        // tag this record as delete.
        result.put(HoodieRecord.HOODIE_IS_DELETED, true);

        org.apache.hudi.utilities.sources.processor.canal.PreCombineFieldType preCombineFieldType =
                valueOf(this.props.getString(CanalJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_TYPE_PROP.key(),
                        CanalJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_TYPE_PROP.defaultValue()).toUpperCase(Locale.ROOT));
        // we can update the `update_time`(delete time) only when it is in timestamp format.
        if (!preCombineFieldType.equals(NON_TIMESTAMP)) {
            String preCombineField = this.props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(),
                    HoodieWriteConfig.PRECOMBINE_FIELD_NAME.defaultValue());

            // ts from maxwell
            long ts = inputJson.get(TS).longValue();

            // convert the `update_time`(delete time) to the proper format.
            if (preCombineFieldType.equals(org.apache.hudi.utilities.sources.processor.maxwell.PreCombineFieldType.DATE_STRING)) {
                // DATE_STRING format
                String timeFormat = this.props.getString(CanalJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_FORMAT_PROP.key(), CanalJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_FORMAT_PROP.defaultValue());
                result.put(preCombineField, DateTimeUtils.formatUnixTimestamp(ts, timeFormat));
            } else if (preCombineFieldType.equals(EPOCHMILLISECONDS)) {
                // EPOCHMILLISECONDS format
                result.put(preCombineField, ts * 1000L);
            } else if (preCombineFieldType.equals(UNIX_TIMESTAMP)) {
                // UNIX_TIMESTAMP format
                result.put(preCombineField, ts);
            } else {
                throw new HoodieSourcePostProcessException("Unsupported preCombine time format " + preCombineFieldType);
            }
        }
        return result.toString();
    }

    public boolean isTargetTable(String database, String table) {
        if (!databaseRegex.isPresent()) {
            return Pattern.matches(tableRegex, table);
        } else {
            return Pattern.matches(databaseRegex.get(), database) && Pattern.matches(tableRegex, table);
        }
    }


    public static class Config {
        public static final ConfigProperty<String> DATABASE_NAME_REGEX_PROP = ConfigProperty
                .key("hoodie.deltastreamer.source.json.kafka.post.processor.canal.database.regex")
                .noDefaultValue()
                .withDocumentation("Database name regex.");

        public static final ConfigProperty<String> TABLE_NAME_REGEX_PROP = ConfigProperty
                .key("hoodie.deltastreamer.source.json.kafka.post.processor.canal.table.regex")
                .noDefaultValue()
                .withDocumentation("Table name regex.");

        public static final ConfigProperty<String> PRECOMBINE_FIELD_TYPE_PROP = ConfigProperty
                .key("hoodie.deltastreamer.source.json.kafka.post.processor.canal.precombine.field.type")
                .defaultValue(DATE_STRING.toString())
                .withDocumentation("Data type of the preCombine field. could be NON_TIMESTAMP, DATE_STRING,"
                        + "UNIX_TIMESTAMP or EPOCHMILLISECONDS. DATE_STRING by default ");

        public static final ConfigProperty<String> PRECOMBINE_FIELD_FORMAT_PROP = ConfigProperty
                .key("hoodie.deltastreamer.source.json.kafka.post.processor.canal.precombine.field.format")
                .defaultValue("yyyy-MM-dd HH:mm:ss")
                .withDocumentation("When the preCombine filed is in DATE_STRING format, use should tell hoodie"
                        + "what format it is. 'yyyy-MM-dd HH:mm:ss' by default");
    }
}
