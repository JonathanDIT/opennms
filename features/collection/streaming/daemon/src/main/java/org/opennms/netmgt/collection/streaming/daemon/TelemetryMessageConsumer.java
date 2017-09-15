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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;
import org.opennms.netmgt.collection.streaming.api.Adapter;
import org.opennms.netmgt.collection.streaming.api.AdapterResult;
import org.opennms.netmgt.collection.streaming.config.Package;
import org.opennms.netmgt.collection.streaming.config.Protocol;
import org.opennms.netmgt.collection.streaming.config.TelemetrydConfigDao;
import org.opennms.netmgt.collection.streaming.ipc.TelemetrySinkModule;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageDTO;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageLogDTO;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.rrd.RrdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opennms.core.utils.InetAddressUtils.addr;

public class TelemetryMessageConsumer implements MessageConsumer<TelemetryMessage, TelemetryMessageLogDTO> {
    private final Logger LOG = LoggerFactory.getLogger(TelemetryMessageConsumer.class);

    private static final ServiceParameters EMPTY_SERVICE_PARAMETERS = new ServiceParameters(Collections.emptyMap());

    @Autowired
    private MessageConsumerManager messageConsumerManager;

    @Autowired
    private TelemetrydConfigDao telemetrydConfigDao;

    @Autowired
    private DistPollerDao distPollerDao;

    @Autowired
    private PersisterFactory persisterFactory;

    @Autowired
    private FilterDao filterDao;

    @Autowired
    private ApplicationContext applicationContext;

    private final Protocol protocolDef;
    private final TelemetrySinkModule sinkModule;
    private final List<Adapter> adapters;

    public TelemetryMessageConsumer(Protocol protocolDef, TelemetrySinkModule sinkModule) throws Exception {
        this.protocolDef = Objects.requireNonNull(protocolDef);
        this.sinkModule = Objects.requireNonNull(sinkModule);
        adapters = new ArrayList<>(protocolDef.getAdapters().size());
    }

    @PostConstruct
    public void setUp() throws Exception {
        // Pre-emptively instantiate the adapters
        for (org.opennms.netmgt.collection.streaming.config.Adapter adapterDef : protocolDef.getAdapters()) {
            try {
                adapters.add(buildAdapter(adapterDef));
            } catch (Exception e) {
                throw new Exception("Failed to create adapter from definition: " + adapterDef, e);
            }
        }
    }

    @Override
    public void handleMessage(TelemetryMessageLogDTO messageLog) {
        LOG.trace("Received message log: {}", messageLog);
        // Handle the message with all of the adapters
        for (Adapter adapter : adapters) {
            for (TelemetryMessageDTO message : messageLog.getMessages()) {
                final AdapterResult result;
                try {
                    result = adapter.handleMessage(messageLog, message);
                } catch (Exception e) {
                    LOG.warn("Failed to handle message: {}. Skipping.", message, e);
                    continue;
                }

                // Locate the matching package definition
                final Package pkg = getPackageFor(result.getAgent());
                if (pkg == null) {
                    LOG.warn("No matching package found for message: {}. Skipping.", message);
                    return;
                }

                // Build the repository from the package definition
                final RrdRepository repository = new RrdRepository();
                repository.setStep(pkg.getRrd().getStep());
                repository.setHeartBeat(repository.getStep() * 2);
                repository.setRraList(pkg.getRrd().getRras());
                repository.setRrdBaseDir(new File(pkg.getRrd().getBaseDir()));

                // Persist!
                final Persister persister = persisterFactory.createPersister(EMPTY_SERVICE_PARAMETERS, repository);
                result.getCollectionSet().visit(persister);
            }
        }
    }

    private Adapter buildAdapter(org.opennms.netmgt.collection.streaming.config.Adapter adapterDef) throws Exception {
        // Instantiate the associated class
        final Class<?> clazz = Class.forName(adapterDef.getClassName());
        final Constructor<?> ctor = clazz.getConstructor();
        final Object adapterInstance = ctor.newInstance();

        // Cast
        if (!(adapterInstance instanceof Adapter)) {
            throw new IllegalArgumentException(String.format("%s must implement %s", adapterDef.getClassName(), Adapter.class.getCanonicalName()));
        }
        final Adapter adapter = (Adapter)adapterInstance;

        // Apply the parameters
        final BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(adapter);
        wrapper.setPropertyValues(Telemetryd.toProperties(adapterDef.getParameters()));

        // Autowire!
        final AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();
        beanFactory.autowireBean(adapter);
        beanFactory.initializeBean(adapter, "adapter");

        return adapter;
    }

    private Package getPackageFor(CollectionAgent agent) {
        for (Package pkg : protocolDef.getPackages()) {
            if (pkg.getFilter() == null || pkg.getFilter().getContent() == null) {
                // No filter specified, always match
                return pkg;
            }
            final String filterRule = pkg.getFilter().getContent();
            if (filterDao.isValid(agent.getHostAddress(), filterRule)) {
                return pkg;
            }
        }
        return null;
    }

    @Override
    public SinkModule<TelemetryMessage, TelemetryMessageLogDTO> getModule() {
        return sinkModule;
    }

    public Protocol getProtocol() {
        return protocolDef;
    }
}
