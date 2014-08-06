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



/*
 * This is really rough. Once it is a little more flushed out will probably merge into the main monitor
 */

package com.pinterest.secor.tools;

import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;

import java.util.HashMap;
import java.util.List;

/**
 * Progress monitor exports offset lags per topic partition.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ProgressMonitorStatsd {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitor.class);
    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaClient mKafkaClient;
    private StatsDClient statsd;

    public ProgressMonitorStatsd(SecorConfig config) {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mKafkaClient = new KafkaClient(mConfig);
        statsd = new NonBlockingStatsDClient(mConfig.getStatsdNamespace(), mConfig.getStatsdHost(), mConfig.getStatsdPort());
    }

    public void exportStats() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        for (String topic : topics) {
            if (topic.matches(mConfig.getTsdbBlacklistTopics()) ||
                    !topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic " + topic);
                continue;
            }
            List<Integer> partitions = mZookeeperConnector.getCommittedOffsetPartitions(topic);
            for (Integer partition : partitions) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                Message committedMessage = mKafkaClient.getCommittedMessage(topicPartition);
                long committedOffset = - 1;
                if (committedMessage == null) {
                    LOG.warn("no committed message found in topic " + topic + " partition " +
                            partition);
                } else {
                    committedOffset = committedMessage.getOffset();
                }

                Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
                if (lastMessage == null) {
                    LOG.warn("no message found in topic " + topic + " partition " + partition);
                } else {
                    long lastOffset = lastMessage.getOffset();
                    assert committedOffset <= lastOffset : Long.toString(committedOffset) + " <= " +
                            lastOffset;
                    long offsetLag = lastOffset - committedOffset;
                    HashMap<String, String> tags = new HashMap<String, String>();
                    tags.put("topic", topic);
                    tags.put("partition", Integer.toString(partition));
                    statsd.gauge(("secor.lag.offsets" + '.' + topic + '.' + Integer.toString(partition)), offsetLag);
                    LOG.debug("topic " + topic + " partition " + partition + " committed offset " +
                            committedOffset + " last offset " + lastOffset);
                }
            }
        }
    }
}
