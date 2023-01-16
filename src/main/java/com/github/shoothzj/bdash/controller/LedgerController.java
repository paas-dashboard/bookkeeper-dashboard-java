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

package com.github.shoothzj.bdash.controller;

import com.github.shoothzj.bdash.config.BookkeeperConfig;
import com.github.shoothzj.bdash.service.LedgerHandleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.LedgerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
@RestController
@RequestMapping("/api/bookkeeper")
public class LedgerController {

    private final BookkeeperConfig config;

    private final BookKeeper bookKeeper;

    private final LedgerHandleService ledgerHandleService;

    public LedgerController(@Autowired BookkeeperConfig config,
                            @Autowired BookKeeper bookKeeper,
                            @Autowired LedgerHandleService ledgerHandleService) {
        this.config = config;
        this.bookKeeper = bookKeeper;
        this.ledgerHandleService = ledgerHandleService;
    }

    @PutMapping("/ledgers")
    public long createLedger() throws BKException, InterruptedException {
        LedgerHandle ledgerHandle = bookKeeper.createLedger(config.ensembleSize,
                config.writeQuorumSize, config.ackQuorumSize, config.digestType, config.getPassword());
        long ledgerId = ledgerHandle.getId();
        ledgerHandleService.putLedgerHandle(ledgerId, ledgerHandle);
        return ledgerId;
    }

    @GetMapping("/ledgers")
    public List<Long> getLedgerList() throws InterruptedException {
        List<Long> result = new ArrayList<>();
        final CountDownLatch processDone = new CountDownLatch(1);
        LedgerManager ledgerManager = bookKeeper.getLedgerManager();
        ledgerManager.asyncProcessLedgers(
                (ledgerId, cb) -> result.add(ledgerId),
                (rc, s, obj) -> {
                    if (rc != BKException.Code.OK) {
                        log.error("exception thrown while get ledger list, error: ", BKException.create(rc));
                    }
                    processDone.countDown();
                },
                null, BKException.Code.OK, BKException.Code.ReadException);
        processDone.await();
        return result;
    }

    @DeleteMapping("/ledgers/{ledger}")
    public ResponseEntity<Void> deleteLedger(@PathVariable long ledger) throws Exception {
        bookKeeper.deleteLedger(ledger);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @PostMapping("/ledgers-delete")
    public ResponseEntity<Void> deleteLedgerList(@RequestBody List<Long> ledgerIds)
            throws BKException, InterruptedException {
        for (long ledgerId : ledgerIds) {
            bookKeeper.deleteLedger(ledgerId);
        }
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
