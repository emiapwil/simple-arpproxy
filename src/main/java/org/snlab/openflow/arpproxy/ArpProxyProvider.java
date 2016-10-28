/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@SuppressWarnings("deprecation")
public class ArpProxyProvider extends AbstractArpProxyComponents implements BindingAwareProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArpProxyProvider.class);

    private ArpProxy proxy;

    private TopologyMonitor monitor;

    private List<ListenerRegistration<?>> reg = Lists.newLinkedList();

    private static final long GRACEFUL_PERIOD = 6000; // 30 seconds

    @Override
    public void onSessionInitiated(ProviderContext session) {
        LOG.info("Initializing ArpProxyProvider");

        DataBroker broker = session.getSALService(DataBroker.class);
        NotificationService notifications = session.getSALService(NotificationService.class);
        PacketProcessingService packetProcessor = session.getRpcService(PacketProcessingService.class);

        proxy = new ArpProxy();
        proxy.setup(broker, packetProcessor);

        monitor = new TopologyMonitor(broker);

        reg.add(broker.registerDataChangeListener(OPERATIONAL, OPENFLOW_TOPOLOGY,
                                                  monitor, DataChangeScope.SUBTREE));

        CompletableFuture.supplyAsync(() -> {
            try {
                Thread.currentThread().wait(GRACEFUL_PERIOD);
            } catch (Exception e) {
            }
            ReadTransaction tx = broker.newReadOnlyTransaction();

            try {
                Topology topology = tx.read(OPERATIONAL, OPENFLOW_TOPOLOGY).get().get();
                monitor.refreshExternalPorts(topology);
            } catch (Exception e) {
                LOG.error("Failed to initialize the external ports!");
            }
            reg.add(notifications.registerNotificationListener(proxy));
            LOG.info("ArpProxyProvider Session Initiated");
            return true;
        });
    }

    @Override
    public void close() throws Exception {
        for (ListenerRegistration<?> registration: reg) {
            try {
                if (registration != null) {
                    registration.close();
                }
            } catch (Exception e) {
                LOG.error("Error when closing registration: {}", e);
            }
        }
        LOG.info("ArpProxyProvider Closed");
    }

}
