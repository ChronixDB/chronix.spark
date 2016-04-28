/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.storage.solr.converter;

import de.qaware.chronix.converter.common.Compression;
import de.qaware.ekg.collector.storage.protocol.ProtocolBuffer;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

/**
 * Small util class to handle the time series format of the EKG
 *
 * @author f.lautenschlager
 */
final class Util {

    private Util() {
        //avoid instances
    }

    /**
     * @param data the chunk of time series data as string (base64 encoded and zipped)
     * @return the points serialized to protocol buffers
     * @throws IOException if the data could not be processed
     */
    public static ProtocolBuffer.Points loadIntoProtocolBuffers(String data) throws IOException {
        byte[] decoded = Base64.decodeBase64(data);
        return ProtocolBuffer.Points.parseFrom(Compression.decompressToStream(decoded));
    }

    /**
     * @param data the data points that are serialized and compressed into a chunk
     * @return a compressed chunk as string
     */
    public static String fromProtocolBuffersToChunk(ProtocolBuffer.Points data) {
        byte[] compressedChunk = Compression.compress(data.toByteArray());
        return Base64.encodeBase64String(compressedChunk);
    }
}
