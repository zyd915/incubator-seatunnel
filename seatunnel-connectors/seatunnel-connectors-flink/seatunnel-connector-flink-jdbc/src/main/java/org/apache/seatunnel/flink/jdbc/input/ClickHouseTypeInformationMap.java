/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.jdbc.input;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.Map;

public class ClickHouseTypeInformationMap implements TypeInformationMap {

    private static final Map<String, TypeInformation<?>> INFORMATION_MAP = new HashMap<>();

    static {
        //Numeric Data Types
        INFORMATION_MAP.put("Int8", BYTE_TYPE_INFO);

        INFORMATION_MAP.put("Int16", SHORT_TYPE_INFO);
        INFORMATION_MAP.put("UInt8", SHORT_TYPE_INFO);

        INFORMATION_MAP.put("Int32", INT_TYPE_INFO);
        INFORMATION_MAP.put("UInt16", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalYear", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalMonth", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalWeek", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalDay", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalHour", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalQuarter", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalMinute", INT_TYPE_INFO);
        INFORMATION_MAP.put("IntervalSecond", INT_TYPE_INFO);

        INFORMATION_MAP.put("Int64", LONG_TYPE_INFO);
        INFORMATION_MAP.put("UInt32", LONG_TYPE_INFO);

        INFORMATION_MAP.put("Int128", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("UInt64", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("UInt128", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("Int256", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("UInt256", BIG_DEC_TYPE_INFO);

        INFORMATION_MAP.put("Float32", FLOAT_TYPE_INFO);
        INFORMATION_MAP.put("Float64", DOUBLE_TYPE_INFO);
        INFORMATION_MAP.put("Decimal", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("Decimal32", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("Decimal64", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("Decimal128", BIG_DEC_TYPE_INFO);
        INFORMATION_MAP.put("Decimal256", BIG_DEC_TYPE_INFO);

        //String Data Types
        INFORMATION_MAP.put("String", STRING_TYPE_INFO);
        INFORMATION_MAP.put("Enum8", STRING_TYPE_INFO);
        INFORMATION_MAP.put("Enum16", STRING_TYPE_INFO);

        INFORMATION_MAP.put("FixedString", STRING_TYPE_INFO);
        INFORMATION_MAP.put("IPv4", STRING_TYPE_INFO);
        INFORMATION_MAP.put("IPv6", STRING_TYPE_INFO);
        INFORMATION_MAP.put("UUID", STRING_TYPE_INFO);

        //Array Data Type
        INFORMATION_MAP.put("Array", BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);

        //Map Data Types
        INFORMATION_MAP.put("Map", STRING_TYPE_INFO);

        //Date and Time Data Types
        INFORMATION_MAP.put("Date", SqlTimeTypeInfo.DATE);
        INFORMATION_MAP.put("DateTime", SqlTimeTypeInfo.TIMESTAMP);
        INFORMATION_MAP.put("DateTime32", SqlTimeTypeInfo.TIMESTAMP);
        INFORMATION_MAP.put("DateTime64", SqlTimeTypeInfo.TIMESTAMP);

        //Other Data Types
        INFORMATION_MAP.put("Tuple", new NothingTypeInfo());
        INFORMATION_MAP.put("Nested", new NothingTypeInfo());
        INFORMATION_MAP.put("AggregateFunction", new NothingTypeInfo());

    }

    @Override
    public TypeInformation<?> getInformation(String datatype) {
        return INFORMATION_MAP.get(datatype);
    }
}
