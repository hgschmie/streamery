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

import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.subscriptions.Subscriptions;

/**
 * JDBI implementation of {@link rx.Observable} to allow data streaming from a query.
 */
public class JdbiObservable<ResultType> implements OnSubscribeFunc<ResultType>
{
    public static <T> Observable<T> from(final Query<T> query)
    {
        return Observable.create(new JdbiObservable<>(query));
    }

    private final Query<ResultType> query;

    private JdbiObservable(final Query<ResultType> query)
    {
        // Register the connection with the handle for cleanup.
        this.query = query.cleanupHandle();
    }

    @Override
    public Subscription onSubscribe(final Observer<? super ResultType> observer)
    {
        // When the result iterator gets closed, it releases all the resources
        // and the connection.
        try (final ResultIterator<ResultType> it = query.iterator()) {
            while (it.hasNext()) {
                observer.onNext(it.next());
            }
            observer.onCompleted();
        }
        catch (final Exception e) {
            observer.onError(e);
        }
        return Subscriptions.empty();
    }
}
