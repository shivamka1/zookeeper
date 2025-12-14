package zookeeper.masterworker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A small helper used alongside ZooKeeper child watchers to track the previous
 * set of children and compute incremental changes.
 *
 * <p>ZooKeeper watchers only tell us that "something changed" under a znode.
 * Without remembering the last observed children set, every watcher event would
 * force us to re-process the entire children list.</p>
 *
 * <p>This cache allows callers to:
 * <ul>
 *   <li>Detect which children were added since the last watcher notification</li>
 *   <li>Detect which children were removed since the last watcher notification</li>
 *   <li>Avoid repeatedly acting on unchanged children</li>
 * </ul>
 *
 * <p>This is especially important in master–worker style coordination, where
 * re-running logic on all children (e.g. tasks or workers) can lead to churn,
 * duplicate work, or the need for complex idempotency handling.</p>
 *
 * <p>The cache is always expected to be updated immediately after processing
 * a watcher event.</p>
 */
public class ChildrenCache {
    protected List<String> children;

    public ChildrenCache() {
        this.children = null;
    }

    public ChildrenCache(List<String> children) {
        this.children = new ArrayList<>(children);
    }

    public boolean isEmpty() {
        return children == null || children.isEmpty();
    }

    public List<String> getList() {
        return children == null ? new ArrayList<>() : new ArrayList<>(children);
    }

    public List<String> refreshCacheAndGetAddedWorkersSinceLastUpdate(List<String> newChildren) {
        ArrayList<String> diff = new ArrayList<>();

        if (children.isEmpty()) {
            diff.addAll(newChildren);
        } else {
            for (String s : newChildren) {
                if (!children.contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.children = new ArrayList<>(newChildren);

        return diff;
    }

    public List<String> refreshCacheAndGetRemovedWorkersSinceLastUpdate(List<String> newChildren) {
        ArrayList<String> diff = new ArrayList<>();

        if (children != null) {
            for (String s : children) {
                if (!newChildren.contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.children = new ArrayList<>(newChildren);

        return diff;
    }
}
