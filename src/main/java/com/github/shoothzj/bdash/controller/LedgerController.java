/**
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

package com.github.shoothzj.bdash.controller;

import com.github.shoothzj.bdash.config.BookkeeperConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/bookkeeper")
public class LedgerController {

    private final BookkeeperConfig config;

    private final BookKeeper bookKeeper;

    public LedgerController(@Autowired BookkeeperConfig config, @Autowired BookKeeper bookKeeper) {
        this.config = config;
        this.bookKeeper = bookKeeper;
    }

    @PutMapping("/ledgers")
    public long createLedger() throws BKException, InterruptedException {
        try (LedgerHandle ledgerHandle = bookKeeper.createLedger(config.ensembleSize,
                config.writeQuorumSize, config.ackQuorumSize, config.digestType, config.getPassword())) {
            return ledgerHandle.getId();
        }
    }
}
