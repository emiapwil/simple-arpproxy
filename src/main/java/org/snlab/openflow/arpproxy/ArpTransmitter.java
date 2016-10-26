/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInputBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

@SuppressWarnings("deprecation")
class ArpTransmitter {

    private static final InstanceIdentifier<Nodes> NODES;

    static {
        NODES = InstanceIdentifier.builder(Nodes.class).build();
    }

    private PacketProcessingService processor;

    public ArpTransmitter(PacketProcessingService processor) {
        this.processor = processor;
    }

    private static String getNodeId(String tid) {
        String[] parts = tid.split(":");
        return parts[0] + ":" + parts[1];
    }

    private static NodeConnectorRef getNodeConnectorRef(String port) {
        String nid = getNodeId(port);

        NodeKey nkey = new NodeKey(new NodeId(nid));
        NodeConnectorKey nckey = new NodeConnectorKey(new NodeConnectorId(port));

        InstanceIdentifier<NodeConnector> iid = NODES.child(Node.class, nkey)
                                                     .child(NodeConnector.class, nckey);
        return new NodeConnectorRef(iid);
    }

    private static TransmitPacketInput createPacketOut(PacketReceived packet, String port) {
        NodeConnectorRef ingress = packet.getIngress();
        NodeConnectorRef egress = getNodeConnectorRef(port);

        NodeKey key = new NodeKey(new NodeId(getNodeId(port)));
        InstanceIdentifier<Node> iid = NODES.child(Node.class, key);
        NodeRef ref = new NodeRef(iid);

        return new TransmitPacketInputBuilder().setPayload(packet.getPayload())
                                               .setNode(ref)
                                               .setEgress(egress)
                                               .setIngress(ingress)
                                               .build();
    }

    public void transmit(PacketReceived packet, String port) {
        TransmitPacketInput out = createPacketOut(packet, port);
        processor.transmitPacket(out);
    }
}
