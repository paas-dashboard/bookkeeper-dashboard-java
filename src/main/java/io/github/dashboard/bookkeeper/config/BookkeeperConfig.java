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

package io.github.dashboard.bookkeeper.config;

import org.apache.bookkeeper.client.BookKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

@Configuration
public class BookkeeperConfig {

    @Value("${BOOKKEEPER_CONNECT_PREFIX:zk+hierarchical}")
    public String connectPrefix;

    @Value("${BOOKKEEPER_ZOOKEEPER_SERVERS:localhost:2181}")
    public String servers;

    @Value("${BOOKKEEPER_DIGESTTYPE:CRC32}")
    public BookKeeper.DigestType digestType;

    @Value("${BOOKKEEPER_PASSWORD:}")
    public String password;

    @Value("${BOOKKEEPER_ENSEMBLE_SIZE:1}")
    public int ensembleSize;

    @Value("${BOOKKEEPER_WRITE_QUORUM_SIZE:1}")
    public int writeQuorumSize;

    @Value("${BOOKKEEPER_ACK_QUORUM_SIZE:1}")
    public int ackQuorumSize;

    public byte[] getPassword() {
        return password.getBytes(StandardCharsets.UTF_8);
    }
}
