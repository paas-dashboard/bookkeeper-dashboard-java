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
import com.github.shoothzj.bdash.module.GetLedgerEntryResp;
import com.github.shoothzj.bdash.util.BkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/bookkeeper")
public class LedgerContentController {

    private final BookkeeperConfig config;

    private final BookKeeper bookKeeper;

    public LedgerContentController(@Autowired BookkeeperConfig config, @Autowired BookKeeper bookKeeper) {
        this.config = config;
        this.bookKeeper = bookKeeper;
    }

    @GetMapping("/ledgers/{ledger}/entries")
    public List<GetLedgerEntryResp> getLedgerEntryList(
            @PathVariable long ledger,
            @RequestParam(value = "codec", required = false) String codec) throws BKException, InterruptedException {
        try (LedgerHandle ledgerHandle = bookKeeper.openLedger(ledger, config.digestType, config.getPassword())) {
            Enumeration<LedgerEntry> readEntries = ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
            List<GetLedgerEntryResp> result = new ArrayList<>();
            while (readEntries.hasMoreElements()) {
                LedgerEntry ledgerEntry = readEntries.nextElement();
                result.add(BkUtil.convert(ledgerEntry, codec));
            }
            return result;
        }
    }

    @GetMapping("/ledgers/{ledger}/lac")
    public long getLedgerLac(@PathVariable long ledger) throws BKException, InterruptedException {
        try (LedgerHandle ledgerHandle = bookKeeper.openLedger(ledger, config.digestType, config.getPassword())) {
            return ledgerHandle.readLastAddConfirmed();
        }
    }

    @GetMapping("/ledgers/{ledger}/last-entry")
    public GetLedgerEntryResp getLedgerLastEntry(@PathVariable long ledger,
                                                 @RequestParam(value = "codec", required = false) String codec)
            throws BKException, InterruptedException {
        try (LedgerHandle ledgerHandle = bookKeeper.openLedger(ledger, config.digestType, config.getPassword())) {
            LedgerEntry ledgerEntry = ledgerHandle.readLastEntry();
            return BkUtil.convert(ledgerEntry, codec);
        }
    }


}
