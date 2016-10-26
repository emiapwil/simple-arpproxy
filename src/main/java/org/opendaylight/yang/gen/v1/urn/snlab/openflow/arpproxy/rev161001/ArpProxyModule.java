package org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001;

import org.snlab.openflow.arpproxy.ArpProxyProvider;

public class ArpProxyModule extends org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.AbstractArpProxyModule {
    public ArpProxyModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public ArpProxyModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver, org.opendaylight.yang.gen.v1.urn.snlab.openflow.arpproxy.rev161001.ArpProxyModule oldModule, java.lang.AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public void customValidation() {
        // add custom validation form module attributes here.
    }

    @Override
    public java.lang.AutoCloseable createInstance() {
        try {
            ArpProxyProvider provider = new ArpProxyProvider();
            getBrokerDependency().registerProvider(provider);
            return provider;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}
