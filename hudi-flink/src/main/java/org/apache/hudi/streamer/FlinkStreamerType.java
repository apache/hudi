/*
 * Copyright (C) 2021 Baidu, Inc. All Rights Reserved.
 */
package org.apache.hudi.streamer;

public enum FlinkStreamerType {

    KAFKA("kafka"),MYSQL_CDC("mysql-cdc");

    private String name;

    FlinkStreamerType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
