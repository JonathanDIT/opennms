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

package org.opennms.netmgt.collection.streaming.itests;

import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.db.MockDatabase;
import org.opennms.core.test.db.annotations.JUnitTemporaryDatabase;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.core.utils.LocationUtils;
import org.opennms.core.xml.JaxbUtils;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.streaming.config.Adapter;
import org.opennms.netmgt.collection.streaming.config.Filter;
import org.opennms.netmgt.collection.streaming.config.Listener;
import org.opennms.netmgt.collection.streaming.config.Package;
import org.opennms.netmgt.collection.streaming.config.Parameter;
import org.opennms.netmgt.collection.streaming.config.Protocol;
import org.opennms.netmgt.collection.streaming.config.Rrd;
import org.opennms.netmgt.collection.streaming.config.TelemetrydConfiguration;
import org.opennms.netmgt.collection.streaming.config.TelemetrydConfigDao;
import org.opennms.netmgt.collection.streaming.daemon.Telemetryd;
import org.opennms.netmgt.collection.streaming.jti.JtiGpbAdapter;
import org.opennms.netmgt.collection.streaming.udp.UdpListener;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.model.NetworkBuilder;
import org.opennms.test.JUnitConfigurationEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionOperations;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations={
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath:/META-INF/opennms/applicationContext-dao.xml",
        "classpath:/META-INF/opennms/applicationContext-commonConfigs.xml",
        "classpath:/META-INF/opennms/applicationContext-minimal-conf.xml",
        "classpath*:/META-INF/opennms/component-dao.xml",
        "classpath*:/META-INF/opennms/component-service.xml",
        "classpath:/META-INF/opennms/applicationContext-pinger.xml",
        "classpath:/META-INF/opennms/applicationContext-daemon.xml",
        "classpath:/META-INF/opennms/mockEventIpcManager.xml",
        "classpath:/META-INF/opennms/applicationContext-queuingservice-mq-vm.xml",
        "classpath:/META-INF/opennms/applicationContext-ipc-sink-server-camel.xml",
        "classpath:/META-INF/opennms/applicationContext-telemetryDaemon.xml"
})
@JUnitConfigurationEnvironment
@JUnitTemporaryDatabase(tempDbClass=MockDatabase.class,reuseDatabase=false)
public class JtiIT {

    @Autowired
    private TelemetrydConfigDao telemetrydConfigDao;

    @Autowired
    private Telemetryd telemetryd;

    @Autowired
    private NodeDao nodeDao;

    @Autowired
    private InterfaceToNodeCache interfaceToNodeCache;

    @Autowired
    private TransactionOperations transOperation;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File rrdBaseDir;

    private AtomicReference<CollectionSet> collectionSetRef = new AtomicReference<>();

    @Before
    public void setUp() throws IOException {
        rrdBaseDir = tempFolder.newFolder("rrd");

        transOperation.execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(final TransactionStatus status) {
                NetworkBuilder nb = new NetworkBuilder();
                nb.addNode("R1")
                        .setForeignSource("Juniper")
                        .setForeignId("1")
                        .setSysObjectId(".1.3.6.1.4.1.9.1.222");
                nb.addInterface("192.0.2.1").setIsSnmpPrimary("P").setIsManaged("P");
                nb.addInterface("172.23.2.12").setIsSnmpPrimary("P").setIsManaged("P");
                nodeDao.save(nb.getCurrentNode());
                nodeDao.flush();
            }
        });

        // Resync after adding nodes/interfaces
        interfaceToNodeCache.dataSourceSync();

        // FIXME: HACK: Should not be necesssary
        interfaceToNodeCache.setNodeId(LocationUtils.DEFAULT_LOCATION_NAME, InetAddressUtils.addr("192.0.2.1"), 1);
        interfaceToNodeCache.setNodeId(LocationUtils.DEFAULT_LOCATION_NAME, InetAddressUtils.addr("172.23.2.12"), 1);
    }

    @Test
    public void canReceivedAndPersistJtiMessages() throws Exception {
        // Use our custom configuration
        updateDaoWithConfig(getConfig());

        // Start the daemon
        telemetryd.start();

        // Send a JTI payload via a UDP socket
        final byte[] jtiMsgBytes = Resources.toByteArray(Resources.getResource("jti_15.1F4_ifd_ae_40000.raw"));
        InetAddress address = InetAddressUtils.getLocalHostAddress();
        DatagramPacket packet = new DatagramPacket(jtiMsgBytes, jtiMsgBytes.length, address, 50000);
        DatagramSocket socket = new DatagramSocket();
        socket.send(packet);

        // Wait until our reference is set
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            return rrdBaseDir.toPath().resolve(Paths.get("1", "ge_0_0_3", "ifOutOctets.jrb")).toFile().canRead();
        }, equalTo(true));
    }

    private void updateDaoWithConfig(TelemetrydConfiguration config) throws IOException {
        final File tempFile = tempFolder.newFile();
        JaxbUtils.marshal(config, tempFile);
        telemetrydConfigDao.setConfigResource(new FileSystemResource(tempFile));
        telemetrydConfigDao.afterPropertiesSet();
    }

    private TelemetrydConfiguration getConfig() {
        TelemetrydConfiguration telemetrydConfig = new TelemetrydConfiguration();

        Protocol jtiProtocol = new Protocol();
        jtiProtocol.setName("JTI");
        jtiProtocol.setDescription("Junos Telemetry Interface (JTI)");
        telemetrydConfig.getProtocols().add(jtiProtocol);

        Listener udpListener = new Listener();
        udpListener.setName("JTI-UDP-50000");
        udpListener.setClassName(UdpListener.class.getCanonicalName());
        // TODO: FIXME: Use dynamic port
        udpListener.getParameters().add(new Parameter("port", "50000"));
        jtiProtocol.getListeners().add(udpListener);

        Package jtiDefaultPkg = new Package();
        jtiDefaultPkg.setName("JTI-Default");
        // TODO: FIXME: Use a filter
        //jtiDefaultPkg.setFilter(new Filter("IPADDR != '0.0.0.0'"));
        jtiProtocol.getPackages().add(jtiDefaultPkg);

        Rrd rrd = new Rrd();
        rrd.setStep(300);
        rrd.setBaseDir(rrdBaseDir.getAbsolutePath());
        rrd.getRras().add("RRA:AVERAGE:0.5:1:2016");
        jtiDefaultPkg.setRrd(rrd);

        Adapter jtiGbpAdapter = new Adapter();
        jtiGbpAdapter.setName("JTI-GBP");
        jtiGbpAdapter.setClassName(JtiGpbAdapter.class.getCanonicalName());

        File script = Paths.get(System.getProperty("opennms.home"),
                "etc", "telemetryd-adapters", "junos-telemetry-interface.groovy").toFile();
        assertTrue("Can't read: " + script.getAbsolutePath(), script.canRead());
        jtiGbpAdapter.getParameters().add(new Parameter("script", script.getAbsolutePath()));
        jtiDefaultPkg.getAdapters().add(jtiGbpAdapter);

        return telemetrydConfig;
    }
}
