/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.main;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.tools.ProgressMonitorStatsd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Progress monitor main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ProgressMonitorMain
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */


/*
 * This is really rough. Once it is a little more flushed out will probably merge into the main monitor
 */


public class ProgressMonitorStatsdMain {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitorStatsdMain.class);

    public static void main(String[] args) {
        try {
            SecorConfig config = SecorConfig.load();
            ProgressMonitorStatsd progressMonitor = new ProgressMonitorStatsd(config);
            Boolean exit = false;
            while(!exit) {
                try {
                    progressMonitor.exportStats();
                    Thread.sleep(60000); //Statsd has ~ 60s resolution
                } catch (InterruptedException e) {
                    LOG.warn("Shutting Down");
                    System.exit(0);
                }
            }
        } catch (Throwable t) {
            LOG.error("Progress monitor failed", t);
            System.exit(1);
        }
    }
}
