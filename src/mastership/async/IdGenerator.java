package mastership.async;


import java.util.UUID;

public final class IdGenerator {

    private IdGenerator() {
        // prevent instantiation
    }

    public static String newId() {
        return UUID.randomUUID().toString();
    }
}
