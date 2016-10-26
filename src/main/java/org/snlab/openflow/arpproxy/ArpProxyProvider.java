/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class ArpProxyProvider implements BindingAwareProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArpProxyProvider.class);

    private ArpProxy proxy;

    @Override
    public void onSessionInitiated(ProviderContext session) {
        LOG.info("Initializing ArpProxyProvider");

        DataBroker broker = session.getSALService(DataBroker.class);
        NotificationService notifications = session.getSALService(NotificationService.class);
        PacketProcessingService packetProcessor = session.getRpcService(PacketProcessingService.class);

        proxy = new ArpProxy();

        proxy.setup(broker, notifications, packetProcessor);

        LOG.info("ArpProxyProvider Session Initiated");
    }

    @Override
    public void close() throws Exception {
        try {
            if (proxy != null) {
                proxy.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing proxy: {}", e);
        }
        LOG.info("ArpProxyProvider Closed");
    }

}
