package zookeeper.masterworker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ChildrenCache {
    protected Optional<List<String>> children;

    public ChildrenCache() {
        this.children = Optional.empty();
    }

    public ChildrenCache(List<String> children) {
        this.children = Optional.of(new ArrayList<>(children));
    }

    public boolean isEmpty() {
        return children.isEmpty();
    }

    public List<String> getList() {
        return children.orElseGet(ArrayList::new);
    }

    public List<String> getAddedSinceLastUpdateAndRefreshCache(List<String> newChildren) {
        ArrayList<String> diff = new ArrayList<>();

        if (children.isEmpty()) {
            diff.addAll(newChildren);
        } else {
            for (String s : newChildren) {
                if (!children.get().contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.children = Optional.of(new ArrayList<>(newChildren));

        return diff;
    }

    public List<String> getRemovedSinceLastUpdateAndRefreshCache(List<String> newChildren) {
        ArrayList<String> diff = new ArrayList<>();

        if (children.isPresent()) {
            for (String s : children.get()) {
                if (!newChildren.contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.children = Optional.of(new ArrayList<>(newChildren));

        return diff;
    }
}
