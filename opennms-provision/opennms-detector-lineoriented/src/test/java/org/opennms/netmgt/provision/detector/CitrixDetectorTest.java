/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2008-2015 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2015 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.provision.detector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.test.MockLogAppender;
import org.opennms.netmgt.provision.DetectFuture;
import org.opennms.netmgt.provision.detector.simple.CitrixDetector;
import org.opennms.netmgt.provision.detector.simple.CitrixDetectorFactory;
import org.opennms.netmgt.provision.server.SimpleServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author Donald Desloge
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/META-INF/opennms/detectors.xml"})
public class CitrixDetectorTest {
    
    @Autowired
    private CitrixDetectorFactory m_detectorFactory;
    
    private CitrixDetector m_detector;
    private SimpleServer m_server;

    @Before
    public void setUp() throws Exception {
        MockLogAppender.setupLogging();

        m_detector = m_detectorFactory.createDetector(new HashMap<>());
        m_detector.setTimeout(500);

        m_server = getServer();
        m_server.init();
        m_server.startServer();


    }

    @After
    public void tearDown() throws IOException {
        if (m_server != null) {
            m_server.stopServer();
            m_server = null;
        }
    }

    @Test(timeout=20000)
    public void testDetectorFailWrongPort() throws Exception {
        m_detector.setPort(65535);
        m_detector.setIdleTime(10000);
        m_detector.init();

        //assertFalse(m_detector.isServiceDetected(m_server.getInetAddress()));
        DetectFuture future = m_detector.isServiceDetected(m_server.getInetAddress());
        assertNotNull(future);
        future.awaitForUninterruptibly();
        assertFalse(future.isServiceDetected());
    }

    @Test(timeout=20000)
    public void testDetectorSuccess() throws Exception {
        m_detector.setPort(m_server.getLocalPort());
        m_detector.setIdleTime(10000);
        m_detector.init();

        //assertTrue(m_detector.isServiceDetected(m_server.getInetAddress()));
        DetectFuture future = m_detector.isServiceDetected(m_server.getInetAddress());
        assertNotNull(future);
        future.awaitForUninterruptibly();
        assertTrue(future.isServiceDetected());
    }

    private SimpleServer getServer() {
        return new SimpleServer() {

            @Override
            public void onInit() {
                setBanner("ICAICAICAICA");
            }
        };
    }
}
