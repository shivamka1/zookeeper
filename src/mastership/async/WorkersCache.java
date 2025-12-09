package mastership.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WorkersCache {
    protected Optional<List<String>> workers;

    public WorkersCache() {
        this.workers = Optional.empty();
    }

    public WorkersCache(List<String> workers) {
        this.workers = Optional.of(new ArrayList<>(workers));
    }

    public boolean isEmpty() {
        return workers.isEmpty();
    }

    public List<String> getList() {
        return workers.orElseGet(ArrayList::new);
    }

    public List<String> getAddedSinceLastUpdateAndRefreshCache(List<String> newWorkers) {
        ArrayList<String> diff = new ArrayList<>();

        if (workers.isEmpty()) {
            diff.addAll(newWorkers);
        } else {
            for (String s : newWorkers) {
                if (!workers.get().contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.workers = Optional.of(new ArrayList<>(newWorkers));

        return diff;
    }

    public List<String> getRemovedSinceLastUpdateAndRefreshCache(List<String> newWorkers) {
        ArrayList<String> diff = new ArrayList<>();

        if (workers.isPresent()) {
            for (String s : workers.get()) {
                if (!newWorkers.contains(s)) {
                    diff.add(s);
                }
            }
        }

        this.workers = Optional.of(new ArrayList<>(newWorkers));

        return diff;
    }
}
