/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tools.command.message;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumeMessageCommandLimitTest {
    @Test
    public void testLimitSpansQueuesWithoutLosingOneMessagePerQueue() throws Exception {
        MessageQueue first = queue(0);
        MessageQueue second = queue(1);
        DefaultMQPullConsumer consumer = consumer(first, 1, second, 2);
        execute(consumer, "-t", "topic", "-c", "2");
        verify(consumer).pull(first, "*", 0L, 1);
        verify(consumer).pull(second, "*", 0L, 1);
        verify(consumer, never()).pull(any(MessageQueue.class), anyString(), eq(1L), anyInt());
    }

    @Test
    public void testStopsBeforeNextQueueWhenLimitIsReached() throws Exception {
        MessageQueue first = queue(0);
        MessageQueue second = queue(1);
        DefaultMQPullConsumer consumer = consumer(first, 1, second, 2);
        execute(consumer, "-t", "topic", "-c", "1");
        verify(consumer).pull(first, "*", 0L, 1);
        verify(consumer, never()).pull(eq(second), anyString(), anyLong(), anyInt());
    }

    @Test
    public void testEmptyQueueDoesNotConsumeTheLimitOrTriggerAPull() throws Exception {
        MessageQueue first = queue(0);
        MessageQueue second = queue(1);
        DefaultMQPullConsumer consumer = consumer(first, 0, second, 2);
        execute(consumer, "-t", "topic", "-c", "1");
        verify(consumer, never()).pull(eq(first), anyString(), anyLong(), anyInt());
        verify(consumer).pull(second, "*", 0L, 1);
    }

    @Test
    public void testExplicitQueueUsesExclusiveMaximumOffset() throws Exception {
        MessageQueue first = queue(0);
        DefaultMQPullConsumer consumer = consumer(first, 2, queue(1), 0);
        execute(consumer, "-t", "topic", "-b", "broker", "-i", "0", "-c", "2");
        verify(consumer).pull(first, "*", 0L, 2);
        verify(consumer, never()).pull(any(MessageQueue.class), anyString(), eq(2L), anyInt());
    }

    @Test
    public void testLargeLimitsDoNotOverflowPullBatchSize() throws Exception {
        MessageQueue first = queue(0);
        long limit = (long) Integer.MAX_VALUE + 1;
        DefaultMQPullConsumer consumer = consumer(first, limit, queue(1), 0);
        execute(consumer, "-t", "topic", "-b", "broker", "-i", "0", "-c", Long.toString(limit));
        verify(consumer).pull(first, "*", 0L, Integer.MAX_VALUE);
        verify(consumer).pull(first, "*", (long) Integer.MAX_VALUE, 1);
    }

    private MessageQueue queue(int id) {
        return new MessageQueue("topic", "broker", id);
    }

    private DefaultMQPullConsumer consumer(MessageQueue first, long firstMax, MessageQueue second,
        long secondMax) throws Exception {
        DefaultMQPullConsumer consumer = mock(DefaultMQPullConsumer.class);
        Set<MessageQueue> queues = new LinkedHashSet<>();
        queues.add(first);
        queues.add(second);
        when(consumer.fetchSubscribeMessageQueues("topic")).thenReturn(queues);
        when(consumer.minOffset(any(MessageQueue.class))).thenReturn(0L);
        when(consumer.maxOffset(any(MessageQueue.class))).thenAnswer(invocation ->
            first.equals(invocation.getArgument(0)) ? firstMax : secondMax);
        when(consumer.pull(any(MessageQueue.class), anyString(), anyLong(), anyInt())).thenAnswer(invocation -> {
            long offset = invocation.getArgument(2);
            int batch = invocation.getArgument(3);
            if (batch <= 0) {
                throw new IllegalArgumentException("non-positive pull batch");
            }
            long max = first.equals(invocation.getArgument(0)) ? firstMax : secondMax;
            if (offset >= max) {
                return new PullResult(PullStatus.NO_NEW_MSG, max, 0, max, null);
            }
            return new PullResult(PullStatus.FOUND, offset + Math.min(batch, max - offset), 0, max,
                Collections.emptyList());
        });
        return consumer;
    }

    private void execute(DefaultMQPullConsumer consumer, String... args) throws Exception {
        ConsumeMessageCommand command = new ConsumeMessageCommand();
        Field field = ConsumeMessageCommand.class.getDeclaredField("defaultMQPullConsumer");
        field.setAccessible(true);
        field.set(command, consumer);
        Options options = command.buildCommandlineOptions(new Options());
        command.execute(new DefaultParser().parse(options, args), options, null);
    }
}
