CREATE TABLE IF NOT EXISTS `schema_registry`.`source_schema_tab`(
    `id` BIGINT,
    `name` STRING,
    `num1` INT,
    `num2` BIGINT,
    `num3` DECIMAL(20,0),
    `num4` TINYINT,
    `num5` FLOAT,
    `num6` DOUBLE,
    `bool` BOOLEAN,
    `bin` BINARY
)