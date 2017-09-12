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
import org.junit.Ignore;
import org.junit.Test;
import org.opennms.netmgt.collection.streaming.jti.proto.LogicalPortOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.Port;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JtiBuilderTest {

    @Test
    @Ignore
    public void canRebuildJtiMessage() throws IOException {
        final byte[] jtiMsgBytes = Resources.toByteArray(Resources.getResource("jti_15.1F4_ifd_ae_40000.raw"));
        System.err.println(jtiMsgBytes.length + " bytes");
        final TelemetryTop.TelemetryStream expectedJtiMsg = TelemetryTop.TelemetryStream.parseFrom(jtiMsgBytes, JtiListener.s_registry);

        final Port.GPort port = Port.GPort.newBuilder()
                .addInterfaceStats(Port.InterfaceInfos.newBuilder()
                        .setIfName("ge-0/0/0")
                        .setInitTime(1457647123)
                        .setSnmpIfIndex(517)
                        .setParentAeName("ae0")
                        .setIngressStats(Port.InterfaceStats.newBuilder()
                                .setIfOctets(1)
                                .build())
                        .setEgressStats(Port.InterfaceStats.newBuilder()
                                .setIfOctets(1)
                                .build())
                        .build())
                .build();

        final TelemetryTop.JuniperNetworksSensors juniperNetworksSensors = TelemetryTop.JuniperNetworksSensors.newBuilder()
                .setExtension(Port.jnprInterfaceExt, port)
                .build();

        final TelemetryTop.EnterpriseSensors sensors = TelemetryTop.EnterpriseSensors.newBuilder()
                .setExtension(TelemetryTop.juniperNetworks, juniperNetworksSensors)
                .build();

        final TelemetryTop.TelemetryStream jtiMsg = TelemetryTop.TelemetryStream.newBuilder()
                .setSystemId("192.0.2.1")
                .setComponentId(0)
                .setSensorName("intf-stats")
                .setSequenceNumber(49103)
                .setTimestamp(1458634993)
                .setEnterprise(sensors)
                .build();

        assertEquals(expectedJtiMsg, jtiMsg);
    }
}
