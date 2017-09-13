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

package org.opennms.netmgt.collection.streaming.jti;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import groovy.lang.Binding;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceException;
import groovy.util.ScriptException;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.collection.api.AttributeType;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.dto.CollectionAgentDTO;
import org.opennms.netmgt.collection.streaming.api.Adapter;
import org.opennms.netmgt.collection.streaming.api.InvalidMessageException;
import org.opennms.netmgt.collection.streaming.jti.proto.CpuMemoryUtilizationOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.FirewallOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.LogicalPortOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.LspMon;
import org.opennms.netmgt.collection.streaming.jti.proto.LspStatsOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.Port;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageDTO;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessageLogDTO;
import org.opennms.netmgt.collection.support.builder.CollectionSetBuilder;
import org.opennms.netmgt.collection.support.builder.InterfaceLevelResource;
import org.opennms.netmgt.collection.support.builder.NodeLevelResource;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class JtiGpbAdapter implements Adapter {

    protected static final ExtensionRegistry s_registry = ExtensionRegistry.newInstance();
    static {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        CpuMemoryUtilizationOuterClass.registerAllExtensions(s_registry);
        FirewallOuterClass.registerAllExtensions(s_registry);
        LogicalPortOuterClass.registerAllExtensions(s_registry);
        LspMon.registerAllExtensions(s_registry);
        LspStatsOuterClass.registerAllExtensions(s_registry);
        Port.registerAllExtensions(s_registry);
        TelemetryTop.registerAllExtensions(s_registry);
    }

    @Autowired
    private CollectionAgentFactory collectionAgentFactory;

    @Autowired
    private InterfaceToNodeCache interfaceToNodeCache;

    @Autowired
    private DistPollerDao distPollerDao;

    private String script;

    @Override
    public CollectionSet convertToCollectionSet(TelemetryMessageLogDTO messageLog, TelemetryMessageDTO message) throws Exception {
        final TelemetryTop.TelemetryStream jtiMsg;
        try {
            jtiMsg = TelemetryTop.TelemetryStream.parseFrom(message.getBytes().array(), s_registry);
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageException("Oops", e);
        }

        // NOTE: In the messages we've seen so far the system id is set to an IP address
        // so we use this to help identify the node, leverage the InterfaceToNodeCache implementation
        final InetAddress inetAddress = InetAddressUtils.addr(jtiMsg.getSystemId());
        final int nodeId = interfaceToNodeCache.getNodeId(messageLog.getLocation(), InetAddressUtils.addr(jtiMsg.getSystemId()));
        // NOTE: This will throw a IllegalArgumentException if the nodeId/inetAddress pair does not exist in the database
        final CollectionAgent agent = collectionAgentFactory.createCollectionAgent(Integer.toString(nodeId), inetAddress);

        CollectionSetBuilder builder = new CollectionSetBuilder(agent);

        /*
        final NodeLevelResource nodeLevelResource = new NodeLevelResource(nodeId);
        final TelemetryTop.EnterpriseSensors sensors = jtiMsg.getEnterprise();
        final TelemetryTop.JuniperNetworksSensors s = sensors.getExtension(TelemetryTop.juniperNetworks);
        final Port.GPort port = s.getExtension(Port.jnprInterfaceExt);
        for (Port.InterfaceInfos interfaceInfos : port.getInterfaceStatsList()) {
            InterfaceLevelResource interfaceResource = new InterfaceLevelResource(nodeLevelResource, interfaceInfos.getIfName());
            builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifInOctets", interfaceInfos.getIngressStats().getIfOctets(), AttributeType.COUNTER);
            builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifOutOctets", interfaceInfos.getEgressStats().getIfOctets(), AttributeType.COUNTER);
        }
        */

        File scriptFile = new File(script);
        GroovyScriptEngine gse = new GroovyScriptEngine(scriptFile.getParentFile().getAbsolutePath());
        Binding binding = new Binding();
        binding.setVariable("agent", agent);
        binding.setVariable("builder", builder);
        binding.setVariable("msg", jtiMsg);
        gse.run(scriptFile.getName(), binding);

        return builder.build();
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }
}
