import groovy.util.logging.Slf4j
import org.opennms.core.utils.RrdLabelUtils
import org.opennms.netmgt.collection.api.AttributeType
import org.opennms.netmgt.collection.api.CollectionAgent
import org.opennms.netmgt.collection.streaming.jti.proto.Port
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop
import org.opennms.netmgt.collection.support.builder.CollectionSetBuilder
import org.opennms.netmgt.collection.support.builder.InterfaceLevelResource
import org.opennms.netmgt.collection.support.builder.NodeLevelResource

@Slf4j
class CollectionSetGenerator {
    CollectionAgent agent
    CollectionSetBuilder builder
    Object msg

    def generate() {
        log.debug("Generating collection set for message: {}", msg)
        NodeLevelResource nodeLevelResource = new NodeLevelResource(agent.getNodeId())

        TelemetryTop.TelemetryStream jtiMsg = msg
        TelemetryTop.EnterpriseSensors entSensors = jtiMsg.getEnterprise()
        TelemetryTop.JuniperNetworksSensors jnprSensors = entSensors.getExtension(TelemetryTop.juniperNetworks);
        Port.GPort port = jnprSensors.getExtension(Port.jnprInterfaceExt);
        for (Port.InterfaceInfos interfaceInfos : port.getInterfaceStatsList()) {
            String interfaceLabel = RrdLabelUtils.computeLabelForRRD(interfaceInfos.getIfName(), null, null);
            InterfaceLevelResource interfaceResource = new InterfaceLevelResource(nodeLevelResource, interfaceLabel);
            builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifInOctets", interfaceInfos.getIngressStats().getIfOctets(), AttributeType.COUNTER);
            builder.withNumericAttribute(interfaceResource, "mib2-interfaces", "ifOutOctets", interfaceInfos.getEgressStats().getIfOctets(), AttributeType.COUNTER);
        }
    }
}

new CollectionSetGenerator(agent: agent, builder: builder, msg: msg).generate()

