package dbradar;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public final class ResourceUtilsClasspathTest {

    private ResourceUtilsClasspathTest() {
    }

    public static void main(String[] args) throws Exception {
        verifiesJarClasspathResourceLoading();
        verifiesFilesystemResourceLoading();
    }

    private static void verifiesJarClasspathResourceLoading() throws Exception {
        Path tempDir = Files.createTempDirectory("resource-utils-jar");
        Path jarPath = tempDir.resolve("sample.jar");
        String resourcePath = "sample/config/test-resource.txt";
        String expected = "jar-resource-content";

        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jarOut.putNextEntry(new JarEntry(resourcePath));
            jarOut.write(expected.getBytes());
            jarOut.closeEntry();
        }

        try (URLClassLoader loader = new URLClassLoader(new URL[]{jarPath.toUri().toURL()}, null)) {
            String actual = ResourceUtils.readText(loader, resourcePath);
            require(expected.equals(actual), "Expected to load resource text from temp jar");
        }
    }

    private static void verifiesFilesystemResourceLoading() throws Exception {
        Path tempFile = Files.createTempFile("resource-utils-file", ".txt");
        Files.writeString(tempFile, "filesystem-resource-content");
        String actual = ResourceUtils.readText(ResourceUtilsClasspathTest.class.getClassLoader(), tempFile.toString());
        require("filesystem-resource-content".equals(actual), "Expected to load resource text from filesystem path");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
