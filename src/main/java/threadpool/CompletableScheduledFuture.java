package threadpool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

public abstract class CompletableScheduledFuture<V> extends CompletableFuture<V> implements ScheduledFuture<V> {

}
