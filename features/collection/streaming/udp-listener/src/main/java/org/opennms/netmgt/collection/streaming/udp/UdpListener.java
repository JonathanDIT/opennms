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

package org.opennms.netmgt.collection.streaming.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.netmgt.collection.streaming.api.Listener;
import org.opennms.netmgt.collection.streaming.model.TelemetryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class UdpListener implements Listener {
    private static final Logger LOG = LoggerFactory.getLogger(UdpListener.class);

    private final AsyncDispatcher<TelemetryMessage> dispatcher;
    private String name;
    private EventLoopGroup bossGroup;
    private ChannelFuture future;

    private int port = 50000;

    public UdpListener(AsyncDispatcher<TelemetryMessage> dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher);
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup();
        final Bootstrap b = new Bootstrap()
                .group(bossGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF, Integer.MAX_VALUE)
                // TODO: Make this configurable
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(8096))
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new MessageToMessageDecoder<DatagramPacket>() {
                            @Override
                            protected void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) throws Exception {
                                // Wrap the contents of the packet in a ByteBuffer, referencing
                                // the underlying byte array if possible
                                final ByteBuffer buffer = wrapContentsWithNioByteBuffer(packet);
                                // Build the message to dispatch via the Sink API
                                final TelemetryMessage msg = new TelemetryMessage(packet.sender(), buffer);
                                // Dispatch and retain a reference to the packet
                                // in the case that we are sharing the underlying byte array
                                final CompletableFuture<TelemetryMessage> future = dispatcher.send(msg);
                                packet.retain();
                                future.whenComplete((res,ex) -> packet.release());
                            }
                        });
                    }
                });
        future = b.bind(port).await();
    }

    public void stop() throws InterruptedException {
        LOG.info("Closing channel...");
        ChannelFuture cf = future.channel().closeFuture();
        LOG.info("Closing boss group...");
        bossGroup.shutdownGracefully().sync();
        cf.sync();
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    protected static ByteBuffer wrapContentsWithNioByteBuffer(DatagramPacket packet) {
        final ByteBuf content = packet.content();
        final int length = content.readableBytes();
        final byte[] array;
        final int offset;
        if (content.hasArray()) {
            array = content.array();
            offset = content.arrayOffset() + content.readerIndex();
        } else {
            array = new byte[length];
            content.getBytes(content.readerIndex(), array, 0, length);
            offset = 0;
        }
        return ByteBuffer.wrap(array, offset, length);
    }
}
