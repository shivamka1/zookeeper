package zookeeper.masterworker.master.tasks;

record TaskCtx(String assignmentPath, String assignedWorker, String taskName, byte[] taskData) {

    TaskCtx(String assignmentPath, String assignedWorker, String taskName) {
        this(assignmentPath, assignedWorker, taskName, new byte[0]);
    }

    TaskCtx withTaskData(byte[] newData) {
        return new TaskCtx(assignmentPath, assignedWorker, taskName, newData);
    }
}
