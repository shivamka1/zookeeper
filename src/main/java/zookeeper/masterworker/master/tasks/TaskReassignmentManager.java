package zookeeper.masterworker.master.tasks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TaskReassignmentManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskReassignmentManager.class);

    private final ZooKeeper zk;

    public TaskReassignmentManager(ZooKeeper zk) {
        this.zk = zk;
    }

    /*
     * When a worker dies, its tasks live under:
     *     /assign/<worker>/<task>
     *
     * We must recover these tasks.
     *
     * We DO NOT assign them directly to another worker here.
     * Instead:
     *
     *     1. Fetch the task data from /assign/<worker>/<task>.
     *     2. Recreate the task under /tasks/<task>.
     *     3. Delete the stale assignment.
     *
     * This ensures the Master detects the task under /tasks (via its watcher)
     * and performs assignment the normal way.
     *
     * This keeps assignment logic centralized and avoids distributed race conditions.
     */
    private final AsyncCallback.ChildrenCallback removedWorkerAssignmentsCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            String worker = (String) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignmentsForRemovedWorker(worker);
                case OK -> {
                    LOG.info("Successfully got a list of assigments: {} tasks", children.size());
                    for (String task : children) {
                        getAssignedTaskData(path + "/" + task, task);
                    }
                }
                default -> LOG.error(
                        "getChildren({}) for absent worker failed",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void getAssignmentsForRemovedWorker(String worker) {
        zk.getChildren(
                "/assign/" + worker,
                false,
                removedWorkerAssignmentsCallback,
                worker
        );
    }

    private final AsyncCallback.DataCallback assignedTaskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignedTaskData(path, (String) ctx);
                case OK -> reassignTask(new TaskCtx(path, (String) ctx, data));
                default -> LOG.error(
                        "Error when getting assigned task data for {}",
                        ctx,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void getAssignedTaskData(String path, String task) {
        zk.getData(
                path,
                false,
                assignedTaskDataCallback,
                task
        );
    }

    private final AsyncCallback.MultiCallback taskReassignMultiCallback = new AsyncCallback.MultiCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            TaskCtx taskCtx = (TaskCtx) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> reassignTask(taskCtx);
                case OK -> LOG.info(
                        "Reassigned task {} from {} back to /tasks/{}",
                        taskCtx.taskName(),
                        taskCtx.path(),
                        taskCtx.taskName()
                );
                default -> LOG.error(
                        "Error when recreating taskName",
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void reassignTask(TaskCtx ctx) {
        String newTaskPath = "/tasks/" + ctx.taskName();

        Op createTaskUnderNewTaskPath = Op.create(
                newTaskPath,
                ctx.taskData(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT
        );

        // ZooKeeper version semantics for delete():
        //   version = -1  → delete unconditionally (ignore the current znode version)
        //   version ≥ 0   → delete only if the current znode version matches
        // Using -1 ensures the assignment is removed regardless of concurrent updates.
        Op deleteTaskFromAssignmentPath = Op.delete(ctx.path(), -1);

        List<Op> ops = Arrays.asList(
                createTaskUnderNewTaskPath,
                deleteTaskFromAssignmentPath
        );

        zk.multi(ops, taskReassignMultiCallback, ctx);
    }

    public void recoverTasks(List<String> removedWorkers) {
        for (String worker : removedWorkers) {
            getAssignmentsForRemovedWorker(worker);
        }
    }
}
