/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2002-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
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

package org.opennms.features.topology.plugins.topo.asset.repo.xml;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * jaxb definition of nodeInfoRepository. This is used for testing 
 * This class also contains static methods for marshalling and unmarshalling XML representations
 * of the nodeInfoRepository to a nodeInfo type. 
 * nodeInfo is a map with values Map<nodeId, Map<nodeParamLabelKey, nodeParamValue>>
 *     nodeParamLabelKey a node asset parameter key (from those defined in org.opennms.plugins.graphml.asset.NodeParamLabels)
 *     nodeParamValue a node asset value ( e.g. key NodeParamLabels.ASSET_RACK ('asset-rack') value: rack1
 *
 */
@XmlRootElement (name="nodeInfoRepository")
@XmlAccessorType(XmlAccessType.NONE)
public class NodeInfoRepositoryXML {

	@XmlElementWrapper(name="nodeInfoList")
	@XmlElement(name="nodeInfo")
	private List<NodeInfoXML> nodeInfoList =  new ArrayList<>();

	public List<NodeInfoXML> getNodeInfoList() {
		return nodeInfoList;
	}

	public void setNodeInfoList(List<NodeInfoXML> nodeInfo) {
		this.nodeInfoList = nodeInfo;
	}
}
