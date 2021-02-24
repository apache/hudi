package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

public class ExtPartitionsKeyGenerator extends SimpleKeyGenerator {

    private String partitionPathField;

    private String partitionPathTemplate;

    public ExtPartitionsKeyGenerator(TypedProperties props) {
        this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
                props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
    }

    ExtPartitionsKeyGenerator(TypedProperties props, String partitionPathField) {
        this(props, null, partitionPathField);
    }

    ExtPartitionsKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField) {
        super(props,recordKeyField,partitionPathField);
        this.partitionPathField = partitionPathField;
        this.partitionPathTemplate = "year={0}/month={1}/day={2}";
    }

    @Override
    public String getPartitionPath(GenericRecord record) {
        String dateVal = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true);
        String[] dateArr = dateVal.split("/");

        String year = dateArr[0];
        String month = dateArr[1];
        String day = dateArr[2];

        return MessageFormat.format(partitionPathTemplate,year,month,day);
    }

    public static void main(String[] args) {
        String year = "1997";
        String month = "03";
        String day = "21";
        System.out.println(MessageFormat.format("year={0}/month={1}/day={2}",year,month,day));
    }


}
