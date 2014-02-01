/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.softwareforge.streamery;

import rx.Observer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stream JSON out of an {@link rx.Observable}. The json contains of an array with the actual streamed
 * values and a boolean (which is at the very end of the json object). This boolean is either true if the
 * data was completely streamed or false if not. If the data was completely streamed, it will also contain
 * a count field with the number of elements streamed.
 */
public class JsonStreamer<T>
    implements Observer<T>
{
    private final JsonGenerator jg;
    private final AtomicInteger count = new AtomicInteger();

    public JsonStreamer(final JsonFactory factory)
        throws IOException
    {
        this.jg = factory.createGenerator(System.out);
        writeStart(jg);
    }

    @Override
    public void onCompleted()
    {
        try {
            writeEnd(jg, count.get(), true);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onError(Throwable t)
    {
        try {
            writeEnd(jg, count.get(), false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onNext(T value)
    {
        try {
            jg.writeObject(value);
            count.incrementAndGet();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeStart(JsonGenerator jg)
        throws IOException
    {
        jg.writeStartObject();
        jg.writeArrayFieldStart("results");
    }

    protected void writeEnd(JsonGenerator jg, int count, boolean success)
        throws IOException
    {
        jg.writeEndArray();
        if (success) {
            jg.writeNumberField("count", count);
        }
        jg.writeBooleanField("success", success);
        jg.writeEndObject();
        jg.flush();
        jg.close();
    }
}
