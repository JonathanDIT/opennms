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

package org.opennms.netmgt.collection.streaming.ipc;

import org.opennms.core.ipc.sink.api.AggregationPolicy;
import org.opennms.core.ipc.sink.api.AsyncPolicy;
import org.opennms.core.ipc.sink.xml.AbstractXmlSinkModule;
import org.opennms.netmgt.collection.streaming.api.TelemetryProtocol;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageDTO;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageLogDTO;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Objects;

public class TelemetrySinkModule extends AbstractXmlSinkModule<TelemetryMessage, TelemetryMessageLogDTO> {
    private static final String MODULE_ID_PREFIX = "Telemetry.";

    @Autowired
    private DistPollerDao distPollerDao;

    private final TelemetryProtocol config;

    private final String moduleId;

    public TelemetrySinkModule(TelemetryProtocol config) {
        super(TelemetryMessageLogDTO.class);
        this.config = Objects.requireNonNull(config);
        this.moduleId = MODULE_ID_PREFIX + config.getName();
    }

    @Override
    public String getId() {
        return moduleId;
    }

    @Override
    public int getNumConsumerThreads() {
        return config.getNumThreads();
    }

    @Override
    public AggregationPolicy<TelemetryMessage, TelemetryMessageLogDTO> getAggregationPolicy() {
        final String systemId = distPollerDao.whoami().getId();
        final String systemLocation = distPollerDao.whoami().getLocation();
        return new AggregationPolicy<TelemetryMessage, TelemetryMessageLogDTO>() {
            @Override
            public int getCompletionSize() {
                return config.getBatchSize();
            }

            @Override
            public int getCompletionIntervalMs() {
                return config.getBatchIntervalMs();
            }

            @Override
            public Object key(TelemetryMessage telemetryMessage) {
                return telemetryMessage.getSource();
            }

            @Override
            public TelemetryMessageLogDTO aggregate(TelemetryMessageLogDTO oldLog, TelemetryMessage message) {
                if (oldLog == null) {
                    oldLog = new TelemetryMessageLogDTO(systemLocation, systemId, message.getSource());
                }
                final TelemetryMessageDTO messageDTO = new TelemetryMessageDTO(message.getBuffer());
                oldLog.getMessages().add(messageDTO);
                return oldLog;
            }
        };
    }

    @Override
    public AsyncPolicy getAsyncPolicy() {
        return new AsyncPolicy() {
            @Override
            public int getQueueSize() {
                return config.getQueueSize();
            }

            @Override
            public int getNumThreads() {
                return config.getNumThreads();
            }

            @Override
            public boolean isBlockWhenFull() {
                return true;
            }
        };
    }

    public DistPollerDao getDistPollerDao() {
        return distPollerDao;
    }

    public void setDistPollerDao(DistPollerDao distPollerDao) {
        this.distPollerDao = distPollerDao;
    }
}
