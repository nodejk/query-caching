package utils;

import java.io.File;
import java.nio.file.Files;

public class FileUtils {

    public static boolean exits(String filePath) {
        return new File(filePath).exists();
    }

    public static boolean createFolder(String folderPath) {
        return new File(folderPath).mkdirs();
    }
}
