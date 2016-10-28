/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ExternalPorts;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ExternalPortsBuilder;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPort;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPortBuilder;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TpId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Link;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.node.TerminationPoint;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import com.google.common.collect.Sets;

final class TopologyMonitor extends AbstractArpProxyComponents implements DataChangeListener {

    private DataBroker broker;

    public TopologyMonitor(DataBroker broker) {
        this.broker = broker;
    }

    private List<Node> getNode(Topology topology) {
        if ((topology == null) || (topology.getNode() == null)) {
            return Collections.emptyList();
        }
        return topology.getNode();
    }

    private List<Link> getLink(Topology topology) {
        if ((topology == null) || (topology.getLink() == null)) {
            return Collections.emptyList();
        }
        return topology.getLink();
    }

    private List<TerminationPoint> getPort(Node node) {
        if ((node == null) || (node.getTerminationPoint() == null)) {
            return Collections.emptyList();
        }
        return node.getTerminationPoint();
    }

    private static boolean isValidPort(String port) {
        return (port.startsWith(OPENFLOW) && !port.endsWith(LOCAL));
    }

    private static ExternalPort createExternalPort(String port) {
        return new ExternalPortBuilder().setPortId(port).build();
    }

    private Set<String> snapshot = Collections.emptySet();

    private ReentrantLock lock = new ReentrantLock();

    public void refreshExternalPorts(Topology topology) {
        Set<String> ports = Sets.newHashSet();

        getNode(topology).forEach((node) -> {
            Set<String> portNames = getPort(node).stream()
                                                 .map(TerminationPoint::getTpId)
                                                 .map(TpId::getValue)
                                                 .filter((port) -> isValidPort(port))
                                                 .collect(Collectors.toSet());
            ports.addAll(portNames);
        });

        getLink(topology).forEach((link) -> {
            String port1 = link.getSource().getSourceTp().getValue();
            String port2 = link.getDestination().getDestTp().getValue();

            if (port1.startsWith(OPENFLOW) && port2.startsWith(OPENFLOW)) {
                ports.remove(port1);
                ports.remove(port2);
            }
        });

        lock.lock();
        if (!Sets.difference(snapshot, ports).isEmpty()) {
            snapshot = ports;
            return;
        }
        lock.unlock();

        List<ExternalPort> externalPorts = ports.stream()
                                                .map(TopologyMonitor::createExternalPort)
                                                .collect(Collectors.toList());
        ExternalPorts updated = new ExternalPortsBuilder().setExternalPort(externalPorts)
                                                          .build();

        WriteTransaction tx = broker.newWriteOnlyTransaction();
        tx.put(OPERATIONAL, EXTERNAL_PORTS, updated);
        tx.submit();
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        if (event.getUpdatedSubtree() instanceof Topology) {
            CompletableFuture.supplyAsync(() -> {
                refreshExternalPorts((Topology)event.getUpdatedSubtree());
                return true;
            });
        }
    }

}
