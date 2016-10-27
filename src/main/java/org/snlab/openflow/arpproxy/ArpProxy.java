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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadTransaction;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev130715.Ipv4Address;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.MacAddress;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev130715.Timestamp;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ExternalPorts;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHost;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHostBuilder;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.KnownHostKey;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPort;
import org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.external.ports.ExternalPortKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

class ArpProxy extends AbstractArpProxyComponents implements PacketProcessingListener {

    private static final long GRACEFUL_PERIOD = 10000; // 10seconds = 10000 millisecond

    private static final int ETHERNET = 0x0001;

    private static final int IPV4 = 0x0800;

    private static final int ARP = 0x0806;

    private static final int VLAN_8021Q = 0x8100;

    private static final int VLAN_8021Q_LEN = 2;

    private static final int OP_REQUEST = 1;

    private static final int OP_REPLY = 2;

    private static final Logger LOG = LoggerFactory.getLogger(ArpProxy.class);

    private static final int MAC_LEN = 6;

    private static final int IPV4_LEN = 4;

    private DataBroker broker;

    private ArpTransmitter transmitter;

    public void setup(DataBroker broker, PacketProcessingService packetProcessor) {
        this.broker = broker;
        this.transmitter = new ArpTransmitter(packetProcessor);

        // Clean up external ports every time
        WriteTransaction tx = broker.newWriteOnlyTransaction();
        tx.delete(OPERATIONAL, EXTERNAL_PORTS);
        tx.submit();
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

    private ListenableFuture<Boolean> fromExternalPort(ReadTransaction tx, PacketReceived packet) {
        String tid = getIngress(packet);

        ExternalPortKey key = new ExternalPortKey(tid);
        InstanceIdentifier<ExternalPort> iid = EXTERNAL_PORTS.child(ExternalPort.class, key);

        return Futures.transform(tx.read(OPERATIONAL, iid), new Function<Optional<?>, Boolean>() {
            public Boolean apply(Optional<?> optional) {
                return optional.isPresent();
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
        KnownHost host = createHost(ip, mac, lastAppear, timestamp);
        updateKnownHost(tx, host);
    }

    private static void updateKnownHost(WriteTransaction tx, KnownHost host) {
        LOG.info("Update host: {} {} {}", host.getIpAddress(),
                                          host.getMacAddress(),
                                          host.getLastUpdate());

        KnownHostKey key = new KnownHostKey(host.getIpAddress());
        InstanceIdentifier<KnownHost> iid = InstanceIdentifier.builder(KnownHost.class, key)
                                                              .build();

        tx.put(OPERATIONAL, iid, host);
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
        future = tx.read(OPERATIONAL, iid);
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

        Futures.transform(tx.read(OPERATIONAL, EXTERNAL_PORTS), new Function<Optional<ExternalPorts>, Boolean>() {
            public Boolean apply(Optional<ExternalPorts> optional) {
                if (!optional.isPresent()) {
                    return false;
                }

                ExternalPorts ports = optional.get();

                for (ExternalPort port: ports.getExternalPort()) {
                    String portId = port.getPortId();
                    forward(packet, portId);
                }
                return true;
            }
        });
    }

    private void forward(PacketReceived packet, String port) {
        LOG.info("Send packet to {}", port);

        if (port.equals(getIngress(packet))) {
            return;
        }
        transmitter.transmit(packet, port);
    }

    private static String getIngress(PacketReceived packet) {
        return packet.getIngress().getValue()
                     .firstKeyOf(NodeConnector.class).getId().getValue();
    }

    private Map<Ipv4Address, AtomicLong> timestamps = Maps.newConcurrentMap();

    private ListenableFuture<Boolean> checkAndLearn(PacketReceived packet, ArpInfo info) {
        Ipv4Address ip = getIpv4Address(info.srcIp);
        long now = System.currentTimeMillis();
        AtomicLong lastUpdate = timestamps.getOrDefault(ip, new AtomicLong(0L));

        long timestamp = lastUpdate.getAndAccumulate(now, (x, y) -> {
            return y - x > GRACEFUL_PERIOD ? y : x;
        });
        if (now - timestamp < GRACEFUL_PERIOD) {
            return Futures.immediateFuture(true);
        }

        final ReadWriteTransaction rwtx = broker.newReadWriteTransaction();
        return Futures.transform(fromExternalPort(rwtx, packet), (Boolean external) -> {
            if (!external) {
                // Ignore ARP from internal links
                LOG.error("Received ARP packets from a neighbor switch!");

                rwtx.cancel();
                return false;
            }

            learn(rwtx, packet, info);
            timestamps.put(ip, lastUpdate);

            rwtx.submit();
            return true;
        });
    }

    private void request(PacketReceived packet, ArpInfo info) {
        // update known hosts
        ReadWriteTransaction rwtx = broker.newReadWriteTransaction();
        long now = System.currentTimeMillis();

        Futures.transform(isKnownHost(rwtx, info), new Function<KnownHost, Boolean>() {
            public Boolean apply(KnownHost host) {
                boolean recent = false;
                if (host != null) {
                    recent = (now - host.getLastUpdate().getValue() < GRACEFUL_PERIOD);

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
}
