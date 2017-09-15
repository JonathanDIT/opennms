/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2017-2017 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2017 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.collection.streaming.daemon;

import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.collection.streaming.api.Adapter;
import org.opennms.netmgt.collection.streaming.api.Listener;
import org.opennms.netmgt.collection.streaming.config.Parameter;
import org.opennms.netmgt.collection.streaming.config.Protocol;
import org.opennms.netmgt.collection.streaming.config.TelemetrydConfigDao;
import org.opennms.netmgt.collection.streaming.config.TelemetrydConfiguration;
import org.opennms.netmgt.collection.streaming.ipc.TelemetrySinkModule;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.daemon.SpringServiceDaemon;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.events.api.annotations.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@EventListener(name=Telemetryd.NAME, logPrefix="telemetryd")
public class Telemetryd implements SpringServiceDaemon {
    private static final Logger LOG = LoggerFactory.getLogger(Telemetryd.class);

    public static final String NAME = "Telemetryd";

    @Autowired
    private TelemetrydConfigDao telemetrydConfigDao;

    @Autowired
    private DistPollerDao distPollerDao;

    @Autowired
    private MessageDispatcherFactory messageDispatcherFactory;

    @Autowired
    private MessageConsumerManager messageConsumerManager;

    @Autowired
    private ApplicationContext applicationContext;

    private List<TelemetryMessageConsumer> consumers = new ArrayList<>();
    private List<Listener> listeners = new ArrayList<>();

    @Override
    public synchronized void start() throws Exception {
        if (consumers.size() > 0) {
            throw new IllegalStateException(NAME + " is already started.");
        }
        final TelemetrydConfiguration config = telemetrydConfigDao.getContainer().getObject();
        final AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();

        for (Protocol protocol : config.getProtocols()) {
            if (!protocol.getEnabled()) {
                LOG.debug("Skipping disabled protocol: {}", protocol.getName());
                continue;
            }
            LOG.debug("Setting up protocol: {}", protocol.getName());

            final TelemetrySinkModule sinkModule = new TelemetrySinkModule(protocol);
            beanFactory.autowireBean(sinkModule);
            beanFactory.initializeBean(sinkModule, "sinkModule");

            final TelemetryMessageConsumer consumer = new TelemetryMessageConsumer(protocol, sinkModule);
            beanFactory.autowireBean(consumer);
            beanFactory.initializeBean(consumer, "consumer");
            consumers.add(consumer);

            final AsyncDispatcher<TelemetryMessage> dispatcher = messageDispatcherFactory.createAsyncDispatcher(sinkModule);
            for (org.opennms.netmgt.collection.streaming.config.Listener listenerDef : protocol.getListeners()) {
                final Listener listener = buildListener(listenerDef, dispatcher);
                listeners.add(listener);
            }
        }

        // Start the consumers
        for (TelemetryMessageConsumer consumer : consumers) {
            LOG.info("Starting consumer for {} protocol.", consumer.getProtocol().getName());
            messageConsumerManager.registerConsumer(consumer);
        }

        // Start the listeners
        for (Listener listener : listeners) {
            LOG.info("Starting {} listener.", listener.getName());
            listener.start();
        }
    }

    protected static Listener buildListener(org.opennms.netmgt.collection.streaming.config.Listener listenerDef, AsyncDispatcher<TelemetryMessage> dispatcher) throws Exception {
        // Instantiate the associated class
        final Class<?> clazz = Class.forName(listenerDef.getClassName());
        final Constructor<?> ctor = clazz.getConstructor(AsyncDispatcher.class);
        final Object listenerInstance = ctor.newInstance(dispatcher);

        // Cast
        if (!(listenerInstance instanceof Listener)) {
            throw new IllegalArgumentException(String.format("%s must implement %s", listenerDef.getClassName(), Listener.class.getCanonicalName()));
        }
        final Listener listener = (Listener)listenerInstance;

        // Apply the parameters
        final BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(listener);
        wrapper.setPropertyValues(toProperties(listenerDef.getParameters()));

        // Update the name
        listener.setName(listenerDef.getName());

        return listener;
    }

    @Override
    public synchronized void destroy() {
        // Stop the consumers
        for (TelemetryMessageConsumer consumer : consumers) {
            try {
                LOG.info("Starting consumer for {} protocol.", consumer.getProtocol().getName());
                messageConsumerManager.unregisterConsumer(consumer);
            } catch (Exception e) {
                LOG.error("Error while stopping consumer.", e);
            }
        }
        consumers.clear();

        // Stop the listeners
        for (Listener listener : listeners) {
            try {
                LOG.info("Stopping {} listener.", listener.getName());
                listener.stop();
            } catch (InterruptedException e) {
                LOG.warn("Error while stopping listener.", e);
            }
        }
        listeners.clear();
    }

    @Override
    public void afterPropertiesSet() {
        // pass
    }

    protected static Map<String, String> toProperties(List<Parameter> parameters) {
        return parameters.stream().collect(
                Collectors.toMap(Parameter::getKey, Parameter::getValue));
    }

}
