/*
 * Copyright © 2015 SNLAB and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.snlab.openflow.arpproxy.utils;

import java.util.List;

public final class RetryExecution {

    private final int retry;

    public RetryExecution(int retry) {
        this.retry = retry;
    }

    public boolean execute(Runnable runnable, List<Class<? extends Exception>> retryReason) {
        for (int i = 0; i < retry; ++i) {
            try {
                runnable.run();

                break;
            } catch (Exception e) {
                boolean quit = true;
                for (Class<? extends Exception> reason: retryReason) {
                    if (reason.isAssignableFrom(e.getClass())) {
                        quit = false;
                        break;
                    }
                }
                if (quit) {
                    return false;
                }
            }
        }

        return true;
    }
}
