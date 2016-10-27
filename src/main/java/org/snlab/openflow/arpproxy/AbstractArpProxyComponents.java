/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ExternalPorts;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

abstract class AbstractArpProxyComponents {
    protected static final String OPENFLOW_NETWORK = "flow:1";
    protected static final InstanceIdentifier<Topology> OPENFLOW_TOPOLOGY;

    protected static final InstanceIdentifier<ExternalPorts> EXTERNAL_PORTS;

    protected static final String OPENFLOW = "openflow";
    protected static final String LOCAL = "LOCAL";

    protected static final LogicalDatastoreType OPERATIONAL = LogicalDatastoreType.OPERATIONAL;

    protected static final LogicalDatastoreType CONFIG = LogicalDatastoreType.CONFIGURATION;


    static {
        TopologyKey key = new TopologyKey(new TopologyId(OPENFLOW_NETWORK));
        OPENFLOW_TOPOLOGY = InstanceIdentifier.builder(NetworkTopology.class)
                                              .child(Topology.class, key)
                                              .build();

        EXTERNAL_PORTS = InstanceIdentifier.builder(ExternalPorts.class)
                                           .build();

    }

}
