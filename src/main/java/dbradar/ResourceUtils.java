package dbradar;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ResourceUtils {

    private ResourceUtils() {
    }

    public static String readText(String path) {
        return readText(ResourceUtils.class.getClassLoader(), path);
    }

    static String readText(ClassLoader loader, String path) {
        Path filePath = Path.of(path);
        if (Files.exists(filePath)) {
            try {
                return Files.readString(filePath);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Failed to read file: %s", path), e);
            }
        }

        InputStream in = loader.getResourceAsStream(path);
        if (in == null) {
            throw new RuntimeException(String.format("Resource does not exist: %s", path));
        }
        try (InputStream stream = in) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read resource: %s", path), e);
        }
    }
}
