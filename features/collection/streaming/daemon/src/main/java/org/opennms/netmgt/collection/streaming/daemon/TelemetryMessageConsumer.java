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
import org.opennms.netmgt.collection.streaming.api.InvalidMessageException;
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

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opennms.core.utils.InetAddressUtils.addr;

public class TelemetryMessageConsumer implements MessageConsumer<TelemetryMessage, TelemetryMessageLogDTO> {
    private final Logger LOG = LoggerFactory.getLogger(TelemetryMessageConsumer.class);

    private final TelemetrySinkModule sinkModule;

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

    public TelemetryMessageConsumer(Protocol protocolDef, TelemetrySinkModule sinkModule) {
        this.protocolDef = Objects.requireNonNull(protocolDef);
        this.sinkModule = Objects.requireNonNull(sinkModule);
    }

    @Override
    public void handleMessage(TelemetryMessageLogDTO messageLog) {
        LOG.debug("Got message log: {}", messageLog);

        // Locate the matching package definition
        // TODO: Maybe cache the results?
        final Package pkg = getPackageFor(messageLog);
        if (pkg == null) {
            LOG.warn("No matching package found for message. Ignoring.");
        }

        // Setup auxiliary objects needed by the persister
        final ServiceParameters params = new ServiceParameters(Collections.emptyMap());

        // TODO: Move this parameters to a configuration file, possibly using
        // a filter expression to determine the repository settings
        final RrdRepository repository = new RrdRepository();
        repository.setStep(pkg.getRrd().getStep());
        repository.setHeartBeat(repository.getStep() * 2);
        repository.setRraList(pkg.getRrd().getRras());
        // This should remain hardcoded
        repository.setRrdBaseDir(new File(pkg.getRrd().getBaseDir()));

        // Now fetch the adapter implementation
        for (org.opennms.netmgt.collection.streaming.config.Adapter adapterDef : pkg.getAdapters()) {
            final Adapter adapter;
            try {
                // TODO: Cache the results
                adapter = buildAdapter(adapterDef);
            } catch (Exception e) {
                LOG.error("Adapter creation failed. Skipping.", e);
                continue;
            }

            for (TelemetryMessageDTO message : messageLog.getMessages()) {
                try {
                    final CollectionSet collectionSet = adapter.convertToCollectionSet(messageLog, message);
                    final Persister persister = persisterFactory.createPersister(params, repository);
                    collectionSet.visit(persister);
                } catch (Exception e) {
                    LOG.error("Oops.", e);
                    continue;
                }
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
        applicationContext.getAutowireCapableBeanFactory().autowireBean(adapter);

        return adapter;
    }

    private Package getPackageFor(TelemetryMessageLogDTO messageLog) {
        for (Package pkg : protocolDef.getPackages()) {
            if (pkg.getFilter() == null || pkg.getFilter().getContent() == null) {
                // No filter specified, always match
                return pkg;
            }

            final String filterRule = pkg.getFilter().getContent();
            // FIXME: We should use the address from the message body
            // and not the source address.
            // FIXME: Avoid casting to string and back to address (see isValid impl)
            if (filterDao.isValid(InetAddressUtils.str(messageLog.getSourceAddress()), filterRule)) {
                return pkg;
            }
        }
        return null;
    }

    @Override
    public SinkModule<TelemetryMessage, TelemetryMessageLogDTO> getModule() {
        return sinkModule;
    }
}
