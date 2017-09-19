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

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.collection.streaming.api.Listener;
import org.opennms.netmgt.collection.streaming.api.TelemetryProtocol;
import org.opennms.netmgt.collection.streaming.ipc.TelemetrySinkModule;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.collection.streaming.udp.UdpListener;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.lang.reflect.Constructor;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
config:edit org.opennms.features.collection.streaming.listeners-udp-50001
config:property-set name JTI
config:property-set class-name org.opennms.netmgt.collection.streaming.udp.UdpListener
config:property-set threads 5
config:property-set queue.size 10000
config:property-set batch.size 1000
config:property-set batch.interval 500
config:property-set listener.port 50001
config:update
 */
public class ListenerFactory implements ManagedServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerFactory.class);

    private BundleContext bundleContext;
    private MessageDispatcherFactory messageDispatcherFactory;
    private DistPollerDao distPollerDao;

    private Map<String, Listener> listenersByPid = new LinkedHashMap<>();

    @Override
    public String getName() {
        return "This Factory creates UDP listeners";
    }

    @Override
    public synchronized void updated(String pid, Dictionary properties) throws ConfigurationException {
        final Listener existingListener = listenersByPid.get(pid);
        if (existingListener != null) {
            LOG.info("Updating existing listener with pid: {}", pid);
            try {
                existingListener.stop();
            } catch (InterruptedException e) {
                LOG.warn("Interruped while stopping listener with pid: {}", pid);
            }
        } else {
            LOG.info("Creating new listener for pid: {}", pid);
        }

        // Convert the dictionary to a map
        final Iterator<String> keysIter = Iterators.forEnumeration(properties.keys());
        final Map<String, String> parameters = Maps.toMap(keysIter, key -> (String)properties.get(key));

        // Build the protocol definition
        final MapBasedTelemetryProtocolDef protocol = new MapBasedTelemetryProtocolDef(parameters);

        // Extract the keys from the map that are prefixed with "listener."
        final Map<String, String> listenersParameters = filterKeysByPrefix(parameters, "listener.");

        final TelemetrySinkModule sinkModule = new TelemetrySinkModule(protocol);
        sinkModule.setDistPollerDao(distPollerDao);
        // FIXME: We need to make sure the dispatcher gets closed too
        final AsyncDispatcher<TelemetryMessage> dispatcher = messageDispatcherFactory.createAsyncDispatcher(sinkModule);

        try {
            final Listener listener = buildListener(protocol.getName(), protocol.getClassName(), listenersParameters, dispatcher);
            listener.start();
            listenersByPid.put(pid, listener);
        } catch (Exception e) {
            LOG.error("Failed to build listener.", e);
        }
    }

    @Override
    public synchronized void deleted(String pid) {
        final Listener listener = listenersByPid.remove(pid);
        if (listener != null) {
            LOG.info("Stopping listener with pid: {}", pid);
            try {
                listener.stop();
            } catch (InterruptedException e) {
                LOG.error("Error occured while stopping listener with pid: {}", pid, e);
            }
        }
    }

    public synchronized void destroy() {
        listenersByPid.keySet().forEach(pid -> deleted(pid));
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

    public BundleContext getBundleContext() {
        return bundleContext;
    }

    public void setBundleContext(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    protected static Map<String, String> filterKeysByPrefix(Map<String, String> map, String prefix) {
        // Extract the keys from the map that are prefixed
        return map.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(Collectors.toMap(k -> k.substring(prefix.length(), k.length()),
                        k -> map.get(k)));
    }

    protected static class MapBasedTelemetryProtocolDef implements TelemetryProtocol {
        private final String name;
        private final String className;
        private final Integer threads;
        private final Integer queueSize;
        private final Integer batchSize;
        private final Integer batchInterval;

        public MapBasedTelemetryProtocolDef(Map<String, String> parameters) {
            name = getString("name", parameters);
            className = getString("class-name", parameters);
            threads = getOptionalInteger("threads", parameters);
            queueSize = getOptionalInteger("queue.size", parameters);
            batchSize = getOptionalInteger("batch.size", parameters);
            batchInterval = getOptionalInteger("batch.interval", parameters);
        }

        @Override
        public String getName() {
            return name;
        }

        public String getClassName() {
            return className;
        }

        @Override
        public Integer getNumThreads() {
            return threads;
        }

        @Override
        public Integer getBatchSize() {
            return batchSize;
        }

        @Override
        public Integer getBatchIntervalMs() {
            return batchInterval;
        }

        @Override
        public Integer getQueueSize() {
            return queueSize;
        }

        private String getString(String key, Map<String, String> parameters) {
            final String value = parameters.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format("%s must be set.", key));
            }
            return value;
        }

        private Integer getOptionalInteger(String key, Map<String, String> parameters) {
            final String strValue = parameters.get(key);
            if (strValue == null) {
                return null;
            }
            return Integer.parseInt(strValue);
        }
    }

}
