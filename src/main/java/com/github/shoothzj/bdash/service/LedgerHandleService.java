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

package com.github.shoothzj.bdash.service;

import org.apache.bookkeeper.client.LedgerHandle;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LedgerHandleService {

    private final Map<Long, LedgerHandle> ledgerHandleMap = new ConcurrentHashMap<>();

    public void putLedgerHandle(long ledgerId, LedgerHandle ledgerHandle) {
        ledgerHandleMap.put(ledgerId, ledgerHandle);
    }

    public LedgerHandle getLedgerHandle(long ledgerId) {
        return ledgerHandleMap.get(ledgerId);
    }

}
