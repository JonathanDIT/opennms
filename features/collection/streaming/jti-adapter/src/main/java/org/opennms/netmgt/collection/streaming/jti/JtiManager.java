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

import org.opennms.core.utils.InetAddressUtils;
import org.opennms.core.utils.LocationUtils;
import org.opennms.netmgt.collection.api.AttributeType;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;
import org.opennms.netmgt.collection.streaming.jti.proto.Port;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;
import org.opennms.netmgt.collection.support.builder.CollectionSetBuilder;
import org.opennms.netmgt.collection.support.builder.InterfaceLevelResource;
import org.opennms.netmgt.collection.support.builder.NodeLevelResource;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.rrd.RrdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

public class JtiManager {
    private static final Logger LOG = LoggerFactory.getLogger(JtiManager.class);

    private final JtiListener jtiListener;
    private PersisterFactory persisterFactory;
    private CollectionAgentFactory collectionAgentFactory;
    private InterfaceToNodeCache interfaceToNodeCache;
    private DistPollerDao distPollerDao;

    private String currentLocation = LocationUtils.DEFAULT_LOCATION_NAME;
    private String script;

    public JtiManager() {
        jtiListener = new JtiListener((jtiMsg) -> handleJtiMessage(jtiMsg));
    }

    /*
        <start-script language="beanshell">
            import org.opennms.netmgt.xml.event.Event;

            msg = bsf.lookupBean("msg");
            builder = bsf.lookupBean("builder");

            final NodeLevelResource nodeLevelResource = new NodeLevelResource(nodeId);
            final TelemetryTop.EnterpriseSensors sensors = jtiMsg.getEnterprise();
            final TelemetryTop.JuniperNetworksSensors s = sensors.getExtension(TelemetryTop.juniperNetworks);
            final Port.GPort port = s.getExtension(Port.jnprInterfaceExt);
            for (Port.InterfaceInfos interfaceInfos : port.getInterfaceStatsList()) {
                InterfaceLevelResource interfaceResource = new InterfaceLevelResource(nodeLevelResource, interfaceInfos.getIfName());
                builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifInOctets", interfaceInfos.getIngressStats().getIfOctets(), AttributeType.COUNTER);
                builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifOutOctets", interfaceInfos.getEgressStats().getIfOctets(), AttributeType.COUNTER);
            }
        </start-script>
     */
    private void handleJtiMessage(TelemetryTop.TelemetryStream jtiMsg) {
        LOG.info("Got JTI message: {}", jtiMsg);

        // NOTE: In the messages we've seen so far the system id is set to an IP address
        // so we use this to help identify the node, leverage the InterfaceToNodeCache implementation
        final InetAddress inetAddress = InetAddressUtils.addr(jtiMsg.getSystemId());
        final int nodeId = interfaceToNodeCache.getNodeId(currentLocation, InetAddressUtils.addr(jtiMsg.getSystemId()));
        // NOTE: This will throw a IllegalArgumentException if the nodeId/inetAddress pair does not exist in the database
        final CollectionAgent agent = collectionAgentFactory.createCollectionAgent(Integer.toString(nodeId), inetAddress);



        // port_interface_ingress_IfOctets{systemId="192.168.1.1",ifName="eth0"} 123123


        LOG.info("Persisting collection set!");
        //collectionSet.visit(persister);
    }

    public void init() throws InterruptedException {
        LOG.info("Starting the JTI listener...");
        currentLocation = distPollerDao.whoami().getLocation();
        jtiListener.start();
        LOG.info("Successfully started the JTI listener...");
    }

    public void destroy() throws InterruptedException {
        LOG.info("Stopping the JTI listener...");
        jtiListener.stop();
        LOG.info("Successfully stopped the JTI listener...");
    }

    public PersisterFactory getPersisterFactory() {
        return persisterFactory;
    }

    public void setPersisterFactory(PersisterFactory persisterFactory) {
        this.persisterFactory = persisterFactory;
    }

    public CollectionAgentFactory getCollectionAgentFactory() {
        return collectionAgentFactory;
    }

    public void setCollectionAgentFactory(CollectionAgentFactory collectionAgentFactory) {
        this.collectionAgentFactory = collectionAgentFactory;
    }

    public InterfaceToNodeCache getInterfaceToNodeCache() {
        return interfaceToNodeCache;
    }

    public void setInterfaceToNodeCache(InterfaceToNodeCache interfaceToNodeCache) {
        this.interfaceToNodeCache = interfaceToNodeCache;
    }

    public DistPollerDao getDistPollerDao() {
        return distPollerDao;
    }

    public void setDistPollerDao(DistPollerDao distPollerDao) {
        this.distPollerDao = distPollerDao;
    }



}
