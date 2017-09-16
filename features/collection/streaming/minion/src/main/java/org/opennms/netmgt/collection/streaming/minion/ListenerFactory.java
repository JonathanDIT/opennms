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

package org.opennms.netmgt.collection.streaming.minion;

import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.collection.streaming.api.Listener;
import org.opennms.netmgt.collection.streaming.api.TelemetryProtocol;
import org.opennms.netmgt.collection.streaming.ipc.TelemetrySinkModule;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.collection.streaming.udp.UdpListener;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.lang.reflect.Constructor;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ListenerFactory implements ManagedServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerFactory.class);

    private MessageDispatcherFactory messageDispatcherFactory;
    private DistPollerDao distPollerDao;

    private Map<String, Listener> listenersByPid = new LinkedHashMap<>();

    @Override
    public String getName() {
        return "This Factory creates UDP listeners";
    }

    @Override
    public void updated(String pid, Dictionary properties) throws ConfigurationException {
        LOG.debug("update({}, {})", pid, properties);

        final TelemetryProtocol protocol = new TelemetryProtocol() {
            @Override
            public String getName() {
                return "JTI";
            }

            @Override
            public Integer getNumThreads() {
                return 1;
            }

            @Override
            public Integer getBatchSize() {
                return 1;
            }

            @Override
            public Integer getBatchIntervalMs() {
                return 0;
            }

            @Override
            public Integer getQueueSize() {
                return 1000;
            }
        };
        final TelemetrySinkModule sinkModule = new TelemetrySinkModule(protocol);
        sinkModule.setDistPollerDao(distPollerDao);
        final AsyncDispatcher<TelemetryMessage> dispatcher = messageDispatcherFactory.createAsyncDispatcher(sinkModule);

        Map<String, String> props = new HashMap<>();
        props.put("port", Integer.toString(50002));
        try {
            final Listener listener = buildListener("JTI-UDP-50002", UdpListener.class.getCanonicalName(), props, dispatcher);
            listener.start();
            listenersByPid.put(pid, listener);
        } catch (Exception e) {
            LOG.error("Failed to build listener.", e);
        }
    }

    @Override
    public void deleted(String pid) {
        LOG.debug("deleted({})", pid);
        final Listener listener = listenersByPid.get(pid);
        if (listener != null) {
            LOG.info("Stopping listener with pid: {}", pid);
            try {
                listener.stop();
            } catch (InterruptedException e) {
                LOG.error("Error occured while stopping listener with pid: {}", pid, e);
            }
        }
    }

    public void init() {
        // Hack
        try {
            updated("init", null);
        } catch (ConfigurationException e) {
            LOG.error("Oops.", e);
        }
    }

    public void stop() {
        // TODO: Stop all
        LOG.debug("stop()");
    }

    // TODO: FIXME: Near duplicate of code in Telemetryd
    protected static Listener buildListener(String name, String className, Map<String, String> properties, AsyncDispatcher<TelemetryMessage> dispatcher) throws Exception {
        // Instantiate the associated class
        final Class<?> clazz = Class.forName(className);
        final Constructor<?> ctor = clazz.getConstructor(AsyncDispatcher.class);
        final Object listenerInstance = ctor.newInstance(dispatcher);

        // Cast
        if (!(listenerInstance instanceof Listener)) {
            throw new IllegalArgumentException(String.format("%s must implement %s", className, Listener.class.getCanonicalName()));
        }
        final Listener listener = (Listener)listenerInstance;

        // Apply the parameters
        final BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(listener);
        wrapper.setPropertyValues(properties);

        // Update the name
        listener.setName(name);

        return listener;
    }

    public MessageDispatcherFactory getMessageDispatcherFactory() {
        return messageDispatcherFactory;
    }

    public void setMessageDispatcherFactory(MessageDispatcherFactory messageDispatcherFactory) {
        this.messageDispatcherFactory = messageDispatcherFactory;
    }

    public DistPollerDao getDistPollerDao() {
        return distPollerDao;
    }

    public void setDistPollerDao(DistPollerDao distPollerDao) {
        this.distPollerDao = distPollerDao;
    }
}
