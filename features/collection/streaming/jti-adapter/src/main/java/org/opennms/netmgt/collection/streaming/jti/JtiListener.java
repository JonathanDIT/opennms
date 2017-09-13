/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2017-2017 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2017 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.collection.streaming.jti;

import com.google.protobuf.ExtensionRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.opennms.netmgt.collection.streaming.jti.proto.CpuMemoryUtilizationOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.FirewallOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.LogicalPortOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.LspMon;
import org.opennms.netmgt.collection.streaming.jti.proto.LspStatsOuterClass;
import org.opennms.netmgt.collection.streaming.jti.proto.Port;
import org.opennms.netmgt.collection.streaming.jti.proto.TelemetryTop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class JtiListener {
    private static final Logger LOG = LoggerFactory.getLogger(JtiListener.class);



    private final Consumer<TelemetryTop.TelemetryStream> handler;
    private EventLoopGroup bossGroup;
    private ChannelFuture future;

    public JtiListener(Consumer<TelemetryTop.TelemetryStream> handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup();
        final Bootstrap b = new Bootstrap()
                .group(bossGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF, Integer.MAX_VALUE)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new MessageToMessageDecoder<DatagramPacket>() {
                            @Override
                            protected void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) throws Exception {
                                out.add(packet.content());
                                packet.retain();
                            }
                        });
                        p.addLast(new ProtobufDecoder( TelemetryTop.TelemetryStream.getDefaultInstance(), JtiGpbAdapter.s_registry));
                        p.addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                if (msg != null && msg instanceof TelemetryTop.TelemetryStream) {
                                    handler.accept((TelemetryTop.TelemetryStream)msg);
                                } else {
                                    LOG.warn("Invalid message!", msg);
                                }
                            }
                        });
                    }
                });
        future = b.bind(50000).await();
    }

    public void stop() throws InterruptedException {
        LOG.info("Closing channel...");
        ChannelFuture cf = future.channel().closeFuture();
        LOG.info("Closing boss group...");
        bossGroup.shutdownGracefully().sync();
        cf.sync();
    }
}
