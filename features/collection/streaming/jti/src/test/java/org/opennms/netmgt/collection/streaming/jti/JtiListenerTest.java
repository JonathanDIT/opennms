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

import com.google.common.io.Resources;
import org.junit.Test;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.collection.streaming.jti.proto.Port;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;

public class JtiListenerTest {

    @Test
    public void canReceivedAndParseJtiMessage() throws InterruptedException, IOException {
        // Start the listener, that sets a reference when a JTI message is received
        final AtomicReference<TelemetryTop.TelemetryStream> jtiMsgRef = new AtomicReference<>();
        JtiListener listener = new JtiListener(jtiMsgRef::set);
        listener.start();

        // Send a JTI payload via a UDP socket
        final byte[] jtiMsgBytes = Resources.toByteArray(Resources.getResource("jti_15.1F4_ifd_ae_40000.raw"));
        InetAddress address = InetAddressUtils.getLocalHostAddress();
        DatagramPacket packet = new DatagramPacket(jtiMsgBytes, jtiMsgBytes.length, address, 50000);
        DatagramSocket socket = new DatagramSocket();
        socket.send(packet);

        // Wait until our reference is set
        await().atMost(1, TimeUnit.MINUTES).until(jtiMsgRef::get, not(nullValue()));

        // Validate the JTI message contents
        TelemetryTop.TelemetryStream jtiMsg = jtiMsgRef.get();
        assertEquals("192.0.2.1", jtiMsg.getSystemId());
        assertEquals(0, jtiMsg.getComponentId());
        assertEquals("intf-stats", jtiMsg.getSensorName());
        assertEquals(49103, jtiMsg.getSequenceNumber());
        assertEquals(1458634993, jtiMsg.getTimestamp());

        TelemetryTop.EnterpriseSensors sensors = jtiMsg.getEnterprise();
        TelemetryTop.JuniperNetworksSensors s = sensors.getExtension(TelemetryTop.juniperNetworks);
        Port.GPort port = s.getExtension(Port.jnprInterfaceExt);

        assertEquals(4, port.getInterfaceStatsList().size());

        // Verify the first interface
        Port.InterfaceInfos interfaceInfos = port.getInterfaceStats(0);
        assertEquals("ge-0/0/0", interfaceInfos.getIfName());
        assertEquals(124827820, interfaceInfos.getIngressStats().getIfOctets());
        assertEquals(194503622, interfaceInfos.getEgressStats().getIfOctets());
    }

}
