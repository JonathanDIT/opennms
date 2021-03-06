/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opennms.container.web.felix.base.internal.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextAttributeListener;

import org.opennms.container.web.felix.base.internal.handler.HandlerRegistry;
import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceFactory;
import org.osgi.framework.ServiceRegistration;

public final class HttpServiceFactory
    implements ServiceFactory<HttpServiceImpl>
{
    private final ServletContext context;
    private final ServletContextAttributeListener attributeListener;
    private final HandlerRegistry handlerRegistry;
    private final boolean sharedContextAttributes;

    public HttpServiceFactory(ServletContext context, HandlerRegistry handlerRegistry,
        ServletContextAttributeListener attributeListener, boolean sharedContextAttributes)
    {
        this.context = context;
        this.attributeListener = attributeListener;
        this.handlerRegistry = handlerRegistry;
        this.sharedContextAttributes = sharedContextAttributes;
    }

    @Override
    public HttpServiceImpl getService(Bundle bundle, ServiceRegistration<HttpServiceImpl> reg)
    {
        return new HttpServiceImpl(
                bundle, this.context, this.handlerRegistry, this.attributeListener,
                this.sharedContextAttributes, getRestAliases());
    }

    @Override
    public void ungetService(Bundle bundle, ServiceRegistration<HttpServiceImpl> reg, HttpServiceImpl service)
    {
        service.unregisterAll();
    }

    protected static Set<String> getRestAliases() {
        final String aliases = System.getProperty("org.opennms.features.osgi.bridge.restAliases", "/rest,/api/v2");
        return getRestAliases(aliases);
    }

    protected static Set<String> getRestAliases(String aliases) {
        if (aliases == null) {
            return new HashSet<>();
        }
        return Arrays.stream(aliases.split(","))
                .filter(alias -> alias != null && !alias.trim().isEmpty())
                .map(alias -> {
                    alias = alias.trim();
                    if (!alias.startsWith("/")) {
                        alias = "/" + alias;
                    }
                    if (!"/".equals(alias) && alias.endsWith("/")) {
                        alias = alias.substring(0, alias.lastIndexOf("/"));
                    }
                    return alias;
                })
                .collect(Collectors.toSet());
    }
}
