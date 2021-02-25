package org.apache.hudi.keygen;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.text.MessageFormat;


/**
 * This Key Generator class is use for data schema is not contain partition schema
 */
@VisibleForTesting
public class InferPartitionsKeyGeneratorForTesting extends SimpleKeyGenerator {

    private String partitionPathField;

    private String partitionPathTemplate;

    public InferPartitionsKeyGeneratorForTesting(TypedProperties props) {
        this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
                props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
    }

    InferPartitionsKeyGeneratorForTesting(TypedProperties props, String partitionPathField) {
        this(props, null, partitionPathField);
    }

    InferPartitionsKeyGeneratorForTesting(TypedProperties props, String recordKeyField, String partitionPathField) {
        super(props, recordKeyField, partitionPathField);
        this.partitionPathField = partitionPathField;
        this.partitionPathTemplate = "year={0}/month={1}/day={2}";
    }

    @Override
    public String getPartitionPath(GenericRecord record) {
        String dateVal = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true);
        String[] dateArr = dateVal.split("/");
        return MessageFormat.format(partitionPathTemplate, dateArr[0], dateArr[1], dateArr[2]);
    }
}
