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

package org.opennms.netmgt.collection.streaming.config;

import org.opennms.netmgt.collection.streaming.api.TelemetryProtocol;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@XmlRootElement(name = "protocol")
@XmlAccessorType(XmlAccessType.NONE)
public class Protocol implements TelemetryProtocol {
    @XmlAttribute(name="name")
    private String name;
    @XmlAttribute(name="description")
    private String description;
    @XmlAttribute(name="enabled")
    private Boolean enabled;
    @XmlAttribute(name="num-threads")
    private Integer numThreads;
    @XmlAttribute(name="batch-size")
    private Integer batchSize;
    @XmlAttribute(name="batch-interval-ms")
    private Integer batchIntervalMs;
    @XmlAttribute(name="queue-size")
    private Integer queueSize;
    @XmlElement(name="listener")
    private List<Listener> listeners = new ArrayList<>();
    @XmlElement(name="package")
    private List<Package> packages = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getEnabled() {
        return enabled != null ? enabled : false;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public Integer getNumThreads() {
        return numThreads != null ? numThreads : Runtime.getRuntime().availableProcessors() * 2;
    }

    public void setNumThreads(Integer numThreads) {
        this.numThreads = numThreads;
    }

    @Override
    public Integer getBatchSize() {
        return batchSize != null ? batchSize : 1000;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public Integer getBatchIntervalMs() {
        return batchIntervalMs != null ? batchIntervalMs : 500;
    }

    public void setBatchIntervalMs(Integer batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public Integer getQueueSize() {
        return queueSize != null ? queueSize : 10000;
    }

    public void setQueueSize(Integer queueSize) {
        this.queueSize = queueSize;
    }

    public List<Listener> getListeners() {
        return listeners;
    }

    public void setListeners(List<Listener> listeners) {
        this.listeners = listeners;
    }

    public List<Package> getPackages() {
        return packages;
    }

    public void setPackages(List<Package> packages) {
        this.packages = packages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Protocol protocol = (Protocol) o;
        return Objects.equals(name, protocol.name) &&
                Objects.equals(description, protocol.description) &&
                Objects.equals(enabled, protocol.enabled) &&
                Objects.equals(numThreads, protocol.numThreads) &&
                Objects.equals(batchSize, protocol.batchSize) &&
                Objects.equals(batchIntervalMs, protocol.batchIntervalMs) &&
                Objects.equals(queueSize, protocol.queueSize) &&
                Objects.equals(listeners, protocol.listeners) &&
                Objects.equals(packages, protocol.packages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, enabled, numThreads, batchSize, batchIntervalMs, queueSize, listeners, packages);
    }

    @Override
    public String toString() {
        return "Protocol{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", enabled=" + enabled +
                ", numThreads=" + numThreads +
                ", batchSize=" + batchSize +
                ", batchIntervalMs=" + batchIntervalMs +
                ", queueSize=" + queueSize +
                ", listeners=" + listeners +
                ", packages=" + packages +
                '}';
    }
}
