/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.ReadTransaction;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev130715.Ipv4Address;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.MacAddress;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.Timestamp;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.InternalProperty;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.InternalPropertyBuilder;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHost;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHostBuilder;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHostKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NodeId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TpId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.link.attributes.Destination;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.link.attributes.Source;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Link;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.NodeKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.node.TerminationPoint;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.node.TerminationPointKey;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snlab.openflow.arpproxy.utils.RetryExecution;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@SuppressWarnings("deprecation")
public class ArpProxy implements PacketProcessingListener, DataChangeListener, AutoCloseable {

    private static final String OPENFLOW = "flow:1";

    private static final long GRACEFUL_PERIOD = 1000; // 10seconds = 1000 millisecond

    private static final int ETHERNET = 0x0001;

    private static final int IPV4 = 0x0800;

    private static final int ARP = 0x0806;

    private static final int VLAN_8021Q = 0x8100;

    private static final int VLAN_8021Q_LEN = 2;

    private static final int OP_REQUEST = 1;

    private static final int OP_REPLY = 2;

    private static final InstanceIdentifier<Topology> OPENFLOW_TOPOLOGY;

    static {
        TopologyKey key = new TopologyKey(new TopologyId(OPENFLOW));

        OPENFLOW_TOPOLOGY = InstanceIdentifier.builder(NetworkTopology.class)
                                              .child(Topology.class, key)
                                              .build();
    }

    private static final Logger LOG = LoggerFactory.getLogger(ArpProxy.class);

    private static final int RETRY_NUMBER = 5;

    private static final int MAC_LEN = 6;

    private static final int IPV4_LEN = 4;

    private DataBroker broker;

    private ArpTransmitter transmitter;

    private List<ListenerRegistration<?>> reg = Lists.newLinkedList();

    public void setup(DataBroker broker, NotificationService notifications,
                      PacketProcessingService packetProcessor) {
        this.broker = broker;
        this.transmitter = new ArpTransmitter(packetProcessor);

        InstanceIdentifier<Link> iid = OPENFLOW_TOPOLOGY.child(Link.class);

        reg.add(broker.registerDataChangeListener(LogicalDatastoreType.OPERATIONAL, iid,
                                                  this, DataChangeScope.ONE));

        reg.add(notifications.registerNotificationListener(this));
    }

    private InstanceIdentifier<InternalProperty> createId(NodeId nid, TpId tid) {
        NodeKey nodeKey = new NodeKey(nid);
        TerminationPointKey tpKey = new TerminationPointKey(tid);
        return OPENFLOW_TOPOLOGY.child(Node.class, nodeKey)
                                .child(TerminationPoint.class, tpKey)
                                .augmentation(InternalProperty.class);
    }

    private void updateTerminationPoint(WriteTransaction tx, NodeId node, TpId tp, boolean internal) {
        if ((node == null) || (tp == null)) {
            return;
        }
        InstanceIdentifier<InternalProperty> iid = createId(node, tp);

        InternalProperty property = new InternalPropertyBuilder().setInternal(internal)
                                                                 .build();

        tx.put(LogicalDatastoreType.OPERATIONAL, iid, property, false);
    }

    private boolean quickCheckNotArp(PacketReceived packet) {
        return false;
    }

    private static MacAddress getMacAddress(byte[] addr) {
        Byte[] bytes = { addr[0], addr[1], addr[2], addr[3], addr[4], addr[5]};
        String mac = Arrays.asList(bytes).stream()
                                         .map(b -> b & 0xff)
                                         .map(i -> String.format("%02x", new Integer(i)))
                                         .collect(Collectors.joining(":"))
                                         .toString();
        return new MacAddress(mac);
    }

    private static Ipv4Address getIpv4Address(byte[] addr) {
        try {
            return new Ipv4Address(InetAddress.getByAddress(addr).getHostAddress());
        } catch (Exception e) {
            return null;
        }
    }

    private static class ArpInfo {
        byte[] srcMac = new byte[MAC_LEN], dstMac = new byte[MAC_LEN];
        byte[] srcIp = new byte[IPV4_LEN], dstIp = new byte[IPV4_LEN];
    }

    private static interface OptionalFunc<I, O> extends Function<Optional<I>, O> {
    }

    private ListenableFuture<Boolean> fromNeighbor(ReadTransaction tx, PacketReceived packet) {
        InstanceIdentifier<?> ingress = packet.getIngress().getValue();
        String tid = ingress.firstKeyOf(NodeConnector.class).getId().getValue();

        TpId tpId = new TpId(tid);
        NodeId node = new NodeId(tpId);

        LOG.info("Reading internal property for node: {}", node);

        ListenableFuture<Optional<InternalProperty>> future;
        future = tx.read(LogicalDatastoreType.OPERATIONAL, createId(node, tpId));
        return Futures.transform(future, new OptionalFunc<InternalProperty, Boolean>() {
            public Boolean apply(Optional<InternalProperty> optional) {
                LOG.info("Property: {}", optional);
                return (optional.isPresent() && optional.get().isInternal());
            }
        });
    }

    private static KnownHost createHost(Ipv4Address ip, MacAddress mac,
                                        String lastAppear, long timestamp) {
        timestamp = timestamp & 0xffffffffL;
        return new KnownHostBuilder().setIpAddress(ip)
                                     .setMacAddress(mac)
                                     .setLastAppear(lastAppear)
                                     .setLastUpdate(new Timestamp(timestamp))
                                     .build();

    }

    private static void updateKnownHost(WriteTransaction tx, Ipv4Address ip, MacAddress mac,
                                        String lastAppear, long timestamp) {
        LOG.info("here: before creating host");
        try {
            KnownHost host = createHost(ip, mac, lastAppear, timestamp);
            LOG.info("here: after creating host");
            updateKnownHost(tx, host);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void updateKnownHost(WriteTransaction tx, KnownHost host) {
        LOG.info("Update host: {} {} {}", host.getIpAddress(),
                                          host.getMacAddress(),
                                          host.getLastUpdate());

        KnownHostKey key = new KnownHostKey(host.getIpAddress());
        InstanceIdentifier<KnownHost> iid = InstanceIdentifier.builder(KnownHost.class, key)
                                                              .build();

        tx.put(LogicalDatastoreType.OPERATIONAL, iid, host);
    }

    private static ListenableFuture<KnownHost> isKnownHost(ReadTransaction tx, ArpInfo info) {
        Ipv4Address ip = getIpv4Address(info.dstIp);

        if (ip == null) {
            return Futures.immediateFuture(null);
        }

        KnownHostKey key = new KnownHostKey(ip);
        InstanceIdentifier<KnownHost> iid = InstanceIdentifier.builder(KnownHost.class, key)
                                                              .build();
        ListenableFuture<Optional<KnownHost>> future;
        future = tx.read(LogicalDatastoreType.OPERATIONAL, iid);
        return Futures.transform(future, new AsyncFunction<Optional<KnownHost>, KnownHost>() {
            public ListenableFuture<KnownHost> apply(Optional<KnownHost> optional) {
                if (optional.isPresent()) {
                    return Futures.immediateFuture(optional.get());
                }
                return Futures.immediateFuture(null);
            }
        });
    }

    private void flood(PacketReceived packet) {
        ReadTransaction tx = broker.newReadOnlyTransaction();

        ListenableFuture<Optional<Topology>> future;
        future = tx.read(LogicalDatastoreType.OPERATIONAL, OPENFLOW_TOPOLOGY);
        Futures.transform(future, new OptionalFunc<Topology, Boolean>() {
            public Boolean apply(Optional<Topology> optional) {
                if (!optional.isPresent()) {
                    LOG.info("Fail to read the topology");
                    return false;
                }
                Topology topology = optional.get();
                if (topology.getNode() == null) {
                    LOG.info("Fail to read the node");
                    return false;
                }

                topology.getNode().forEach((node) -> {
                    CompletableFuture.supplyAsync(() -> {
                        if (node.getTerminationPoint() == null) {
                            LOG.info("Can't find the termination point for {}", node);
                            return false;
                        }
                        node.getTerminationPoint().forEach((tp) -> {
                            if (tp.getTpId().getValue().endsWith("LOCAL")) {
                                return;
                            }
                            InternalProperty itp = tp.getAugmentation(InternalProperty.class);
                            if ((itp == null) || (!itp.isInternal())) {
                                forward(packet, tp.getTpId().getValue());
                            }
                        });
                        return true;
                    });
                });
                return true;
            }
        });
    }

    private void forward(PacketReceived packet, String port) {
        LOG.info("Send packet to {}", port);
        transmitter.transmit(packet, port);
    }

    private static String getIngress(PacketReceived packet) {
        return packet.getIngress().getValue()
                     .firstKeyOf(NodeConnector.class).getId().getValue();
    }

    private ListenableFuture<Boolean> checkAndLearn(PacketReceived packet, ArpInfo info) {
        final ReadWriteTransaction rwtx = broker.newReadWriteTransaction();
        return Futures.transform(fromNeighbor(rwtx, packet), (Boolean fromNeighbor) -> {
            if (fromNeighbor) {
                // Ignore ARP from internal links
                LOG.error("Received ARP packets from a neighbor switch!");

                rwtx.cancel();
                return false;
            }

            LOG.info("ARP packet from a potential host");

            learn(rwtx, packet, info);

            rwtx.submit();
            return true;
        });
    }

    private void request(PacketReceived packet, ArpInfo info) {
        // update known hosts
        ReadWriteTransaction rwtx = broker.newReadWriteTransaction();
        long now = System.currentTimeMillis() / 10;

        Futures.transform(isKnownHost(rwtx, info), new Function<KnownHost, Boolean>() {
            public Boolean apply(KnownHost host) {
                boolean recent = false;
                if (host != null) {
                    recent = (now - host.getLastUpdate().getValue() > GRACEFUL_PERIOD);

                    LOG.info("Host is recently updated: ", host.getLastUpdate());

                    rwtx.cancel();
                } else {
                    Ipv4Address ip = getIpv4Address(info.dstIp);
                    MacAddress mac = getMacAddress(info.dstMac);

                    LOG.info("Querying {}/{}", ip, mac);

                    host = createHost(ip, mac, null, now);
                    updateKnownHost(rwtx, host);

                    rwtx.submit();
                }
                // No retry, just get on all fours and pray for the best.

                final String lastAppear = host.getLastAppear();
                final boolean canFlood = !recent;
                CompletableFuture.supplyAsync(() -> {
                    if (lastAppear == null) {
                        LOG.info("Flood the request");
                        if (canFlood) {
                            flood(packet);
                        }
                    } else {
                        LOG.info("Forward the request");
                        forward(packet, lastAppear);
                    }
                    return true;
                });
                return true;
            }
        });
    }

    private void reply(PacketReceived packet, ArpInfo info) {
        Ipv4Address ip = getIpv4Address(info.dstIp);
        MacAddress mac = getMacAddress(info.dstMac);

        ReadTransaction rtx = broker.newReadOnlyTransaction();
        Futures.transform(isKnownHost(rtx, info), new Function<KnownHost, Boolean>() {
            public Boolean apply(KnownHost host) {
                if (host == null) {
                    LOG.error("Replying to a non-existing sender: {} {}", ip, mac);
                    return false;
                }
                if (host.getLastAppear() == null) {
                    LOG.error("Cannot reply to a ghost host: {} {}", ip, mac);
                    return false;
                }
                forward(packet, host.getLastAppear());
                return true;
            }
        });
    }

    private void learn(WriteTransaction wtx, PacketReceived packet, ArpInfo info) {
        Ipv4Address ip = getIpv4Address(info.srcIp);
        MacAddress mac = getMacAddress(info.srcMac);
        LOG.info("Learning address binding: {} - {}", ip, mac);

        String ingress = getIngress(packet);
        long now = System.currentTimeMillis();

        updateKnownHost(wtx, ip, mac, ingress, now);
    }

    @Override
    public void onPacketReceived(PacketReceived packet) {
        CompletableFuture.supplyAsync(() -> {
            if (quickCheckNotArp(packet)) {
                return false;
            }

            byte[] bytes = packet.getPayload();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            int offset = 2 * MAC_LEN + 2;

            int etherType = buffer.getShort(2 * MAC_LEN) & 0xffff;
            if (etherType == VLAN_8021Q) {
                etherType = buffer.getShort(2 * MAC_LEN + VLAN_8021Q_LEN) & 0xffff;
                offset += VLAN_8021Q_LEN;
            }

            if (etherType != ARP) {
                return false;
            }
            buffer.position(offset);

            int hwType = buffer.getShort() & 0xffff;
            int netType = buffer.getShort() & 0xffff;
            int hlen = buffer.get() & 0xff;
            int plen = buffer.get() & 0xff;

            if ((hwType != ETHERNET) || (netType != IPV4)) {
                LOG.info("Only support ethernet and IPv4 at the time");
                return false;
            }
            if ((hlen != 6) || (plen != 4)) {
                LOG.info("Lengths for protocols are not correct!");
                return false;
            }

            int op = buffer.getShort() & 0xffff;

            ArpInfo info = new ArpInfo();
            buffer.get(info.srcMac);
            buffer.get(info.srcIp);

            buffer.get(info.dstMac);
            buffer.get(info.dstIp);

            LOG.info("Ready to handle ARP packets!");

            Futures.transform(checkAndLearn(packet, info), new Function<Boolean, Boolean>() {
                public Boolean apply(Boolean checked) {
                    if (!checked) {
                        LOG.info("Packet is from neighbor, ignore");
                        return false;
                    }

                    CompletableFuture.supplyAsync(() -> {
                        if (op == OP_REQUEST) {
                            LOG.info("Handling ARP request");
                            request(packet, info);
                        } else if (op == OP_REPLY) {
                            LOG.info("Handling ARP reply");
                            reply(packet, info);
                        } else {
                            LOG.error("Invalid operation number: {}", op);
                            return false;
                        }
                        return true;
                    });
                    return true;
                }
            });
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
    }

    private boolean isOpenflowSwitch(NodeId node) {
        String nodeId = node.getValue();
        return (nodeId.startsWith("openflow"));
    }

    private void deleteLink(WriteTransaction tx, AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        for (InstanceIdentifier<?> iid: event.getRemovedPaths()) {
            DataObject obj = event.getOriginalData().get(iid);

            if (!(obj instanceof Link)) {
                continue;
            }

            Link link = (Link) obj;
            Source src = link.getSource();
            Destination dst = link.getDestination();

            if ((src == null) || (dst == null)) {
                continue;
            }

            updateTerminationPoint(tx, src.getSourceNode(), src.getSourceTp(), false);
            updateTerminationPoint(tx, dst.getDestNode(), dst.getDestTp(), false);
        }
    }

    private void createLink(WriteTransaction tx, AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        for (DataObject obj: event.getCreatedData().values()) {
            if (!(obj instanceof Link)) {
                continue;
            }

            Link link = (Link) obj;

            Source src = link.getSource();
            Destination dst = link.getDestination();

            if ((src == null) || (dst == null)) {
                continue;
            }

            if (isOpenflowSwitch(src.getSourceNode()) && isOpenflowSwitch(dst.getDestNode())) {
                updateTerminationPoint(tx, src.getSourceNode(), src.getSourceTp(), true);
                updateTerminationPoint(tx, dst.getDestNode(), dst.getDestTp(), true);
            }
        }
    }

    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        CompletableFuture.supplyAsync(() -> {
            return new RetryExecution(RETRY_NUMBER).execute(() -> {
                WriteTransaction wtx = broker.newWriteOnlyTransaction();
                try {

                    deleteLink(wtx, event);
                    createLink(wtx, event);

                    wtx.submit();
                } catch (Exception e) {
                    wtx.cancel();
                }
            }, Arrays.asList(TransactionCommitFailedException.class));
        });
    }

}
