/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.dashboard.bookkeeper.util;

import io.github.dashboard.bookkeeper.module.DecodeComponent;
import io.github.dashboard.bookkeeper.module.DecodeNamespace;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.protocol.pulsar.codec.mledger.MLDataFormats;

import java.nio.charset.StandardCharsets;

public class DecodeUtil {

    public static String decodeData(byte[] data, String component, String namespace) throws Exception {
        DecodeComponent decodeComponent = DecodeComponent.valueOf(component);
        switch (decodeComponent) {
            case Pulsar:
                return decodePulsarData(data, namespace);
            default:
                return new String(data, StandardCharsets.UTF_8);
        }
    }

    public static String decodePulsarData(byte[] data, String namespace) throws Exception {
        DecodeNamespace decodeNamespace = DecodeNamespace.valueOf(namespace);
        switch (decodeNamespace) {
            case ManagedLedgerTopic:
                return decodePulsarManagedLedgerTopicData(data).toString();
            case ManagedManagedLedgerSubscription:
                return decodePulsarManagedLedgerSubscriptionData(data).toString();
            default:
                return new String(data, StandardCharsets.UTF_8);
        }
    }

    public static MLDataFormats.ManagedLedgerInfo decodePulsarManagedLedgerTopicData(byte[] data)
            throws InvalidProtocolBufferException {
        return MLDataFormats.ManagedLedgerInfo.parseFrom(data);
    }

    public static MLDataFormats.ManagedCursorInfo decodePulsarManagedLedgerSubscriptionData(byte[] data)
            throws InvalidProtocolBufferException {
        return MLDataFormats.ManagedCursorInfo.parseFrom(data);
    }

}
