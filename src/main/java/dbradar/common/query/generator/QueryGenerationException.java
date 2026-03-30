package dbradar.common.query.generator;

public class QueryGenerationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryGenerationException() {
    }

    public QueryGenerationException(String message) {
        super(message);
    }
}
