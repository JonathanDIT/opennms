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

import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JtiManager {
    private static final Logger LOG = LoggerFactory.getLogger(JtiManager.class);

    private final JtiListener jtiListener;
    private PersisterFactory persisterFactory;
    private CollectionAgentFactory collectionAgentFactory;

    public JtiManager() {
        jtiListener = new JtiListener((jtiMsg) -> handleJtiMessage(jtiMsg));
    }

    private void handleJtiMessage(TelemetryTop.TelemetryStream jtiMsg) {
        LOG.info("Got JTI message: {}", jtiMsg);
    }

    public void init() throws InterruptedException {
        LOG.info("Starting the JTI listener...");
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
}
