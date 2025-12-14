package zookeeper.masterworker.master.tasks;

public record TaskCtx(String assignmentPath, String assignedWorker, String taskName, byte[] taskData) {

    public TaskCtx(String assignmentPath, String assignedWorker, String taskName) {
        this(assignmentPath, assignedWorker, taskName, new byte[0]);
    }

    public TaskCtx withTaskData(byte[] newData) {
        return new TaskCtx(assignmentPath, assignedWorker, taskName, newData);
    }
}
