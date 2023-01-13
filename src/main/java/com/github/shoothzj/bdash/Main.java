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

package com.github.shoothzj.bdash;

import com.github.shoothzj.bdash.config.BookkeeperConfig;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.io.File;

@SpringBootApplication
public class Main {

    private final String staticPath;

    {
        String aux = System.getenv("STATIC_PATH");
        if (aux == null) {
            staticPath = "";
        } else if (aux.endsWith(File.separator)) {
            staticPath = aux;
        } else {
            staticPath = aux + File.separator;
        }
    }

    @Bean
    RouterFunction<ServerResponse> homePageRouter() {
        return RouterFunctions.route(RequestPredicates.GET("/"),
                request -> ServerResponse.ok().bodyValue(new FileSystemResource(staticPath + "index.html")));
    }

    @Bean
    RouterFunction<ServerResponse> staticResourceRouter() {
        return RouterFunctions.resources("/**", new FileSystemResource(staticPath));
    }

    @Bean
    BookKeeper createBookKeeper(@Autowired BookkeeperConfig bookkeeperConfig) throws Exception {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMetadataServiceUri("zk+null://" + bookkeeperConfig.servers + "/ledgers");
        return new BookKeeper(clientConfiguration);
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

}
