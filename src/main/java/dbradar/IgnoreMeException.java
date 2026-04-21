package dbradar;

public class IgnoreMeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IgnoreMeException() {
    }

    public IgnoreMeException(String message) {
        super(message);
    }
}
