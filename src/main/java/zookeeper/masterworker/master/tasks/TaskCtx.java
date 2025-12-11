package zookeeper.masterworker.master.tasks;

public record TaskCtx(String path, String taskName, byte[] taskData) {
}
