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

package com.github.shoothzj.bdash.util;

import com.github.shoothzj.bdash.module.GetLedgerEntryResp;
import org.apache.bookkeeper.client.LedgerEntry;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

public class BkUtil {

    public static GetLedgerEntryResp convert(LedgerEntry ledgerEntry, @Nullable String codec) {
        GetLedgerEntryResp getLedgerEntryResp = new GetLedgerEntryResp();
        getLedgerEntryResp.setLedgerId(ledgerEntry.getLedgerId());
        getLedgerEntryResp.setEntryId(ledgerEntry.getEntryId());
        getLedgerEntryResp.setLength(ledgerEntry.getLength());
        getLedgerEntryResp.setContent(getContent(ledgerEntry.getEntry(), codec));
        return getLedgerEntryResp;
    }

    private static String getContent(byte[] data, @Nullable String codec) {
        if ("hex".equalsIgnoreCase(codec)) {
            return HexUtil.bytes2hex(data);
        } else {
            return new String(data, StandardCharsets.UTF_8);
        }
    }

}
