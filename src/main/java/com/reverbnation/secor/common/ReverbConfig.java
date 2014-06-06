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
package com.reverbnation.secor.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

/**
 * One-stop shop for Secor configuration options.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ReverbConfig {
    private final PropertiesConfiguration mProperties;

    public static ReverbConfig load() throws ConfigurationException {
        // Load the default configuration file first
        String configProperty = System.getProperty("config");
        PropertiesConfiguration properties = new PropertiesConfiguration(configProperty);

        return new ReverbConfig(properties);
    }

    private ReverbConfig(PropertiesConfiguration properties) {
        mProperties = properties;
    }

    public String getSchemaRegistryUri() {
        return getString("reverb.schema.registry.uri");
    }

    private void checkProperty(String name) {
        if (!mProperties.containsKey(name)) {
            throw new RuntimeException("Failed to find required configuration option '" +
                                       name + "'.");
        }
    }

    private String getString(String name) {
        checkProperty(name);
        return mProperties.getString(name);
    }

    private int getInt(String name) {
        checkProperty(name);
        return mProperties.getInt(name);
    }

    private long getLong(String name) {
        return mProperties.getLong(name);
    }

    private String[] getStringArray(String name) {
        return mProperties.getStringArray(name);
    }
}
