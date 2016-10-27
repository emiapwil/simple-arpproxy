/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ExternalPorts;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPort;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPortBuilder;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPortKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Link;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

final class TopologyMonitor implements DataChangeListener {

    private static final LogicalDatastoreType OPERATIONAL = LogicalDatastoreType.OPERATIONAL;

    private static final InstanceIdentifier<ExternalPorts> EXTERNAL_PORTS;

    static {
        EXTERNAL_PORTS = InstanceIdentifier.builder(ExternalPorts.class)
                                           .build();

    }

    private DataBroker broker;

    private static boolean isOpenflowSwitch(String nid) {
        return (nid.startsWith("openflow"));
    }

    public TopologyMonitor(DataBroker broker) {
        this.broker = broker;
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        CompletableFuture.supplyAsync(() -> {
            ReadWriteTransaction tx = broker.newReadWriteTransaction();
            try {
                for (InstanceIdentifier<?> iid: event.getRemovedPaths()) {
                    delete(tx, event.getOriginalData().get(iid));
                }
                for (DataObject obj: event.getUpdatedData().values()) {
                    modify(tx, obj);
                }
                for (DataObject obj: event.getCreatedData().values()) {
                    create(tx, obj);
                }

                wtx.submit().get();
            } catch (Exception e) {
                wtx.cancel();
            }
            return true;
        });
    }

    private ListenableFuture<Boolean> update(ReadWriteTransaction tx, String port, boolean external) {
        if (!isOpenflowSwitch(port)) {
            return Futures.immediateFuture(true);
        }
        ExternalPortKey key = new ExternalPortKey(port);
        InstanceIdentifier<ExternalPort> iid = EXTERNAL_PORTS.child(ExternalPort.class, key);
        return Futures.transform(tx.read(OPERATIONAL, iid), new Function<Optional<ExternalPort>, Boolean>() {
            public Boolean apply(Optional<ExternalPort> optional) {
                if (optional.isPresent()) {
                    if (external == true) {
                        tx.delete(OPERATIONAL, iid);
                    }
                } else {
                    if (external == false) {
                        tx.put(OPERATIONAL, iid, new ExternalPortBuilder().setPortId(port).build());
                    }
                }
                return true;
            }
        });
    }

    private List<ListenableFuture<Boolean>> deleteLink(ReadWriteTransaction tx, DataObject obj) {
        if (!(obj instanceof Link)) {
            return Arrays.asList(Futures.immediateFuture(true));
        }
        Link link = (Link) obj;

        String src = link.getSource().getSourceTp().getValue();
        String dst = link.getDestination().getDestTp().getValue();

        ListenableFuture<Boolean> f1 = Futures.immediateFuture(true);
        ListenableFuture<Boolean> f2 = Futures.immediateFuture(true);

        if (isOpenflowSwitch(src)) {
            f1 = update(tx, src, false);
        }
        if (isOpenflowSwitch(dst)) {
            f2 = update(tx, dst, false);
        }
        return Arrays.asList(f1, f2);
    }

    private List<ListenableFuture<Boolean>> createLink(ReadWriteTransaction tx, DataObject obj) {
        if (!(obj instanceof Link)) {
            return Arrays.asList(Futures.immediateFuture(true));
        }

        Link link = (Link) obj;

        String src = link.getSource().getSourceTp().getValue();
        String dst = link.getDestination().getDestTp().getValue();

        List<ListenableFuture<Boolean>> futures = Lists.newLinkedList();
        if (isOpenflowSwitch(src)) {
            if (isOpenflowSwitch(dst)) {
                futures.add(update(tx, src, false));
                futures.add(update(tx, dst, false));
            } else {
                futures.add(update(tx, src, true));
            }
        } else {
            if (isOpenflowSwitch(dst)) {
                futures.add(update(tx, dst, true));
            }
        }
        return futures;
    }

}
