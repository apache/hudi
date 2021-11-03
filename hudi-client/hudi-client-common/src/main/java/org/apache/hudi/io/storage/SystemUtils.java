package org.apache.hudi.io.storage;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;

/**
 * system util.
 */
public class SystemUtils {

  public static URL getClassLocation(final Class<?> cls) {

    if (cls == null) {
      throw new IllegalArgumentException("null input: cls");
    }

    URL result = null;
    final String clsAsResource = cls.getName().replace('.', '/').concat(".class");

    final ProtectionDomain pd = cls.getProtectionDomain();

    if (pd != null) {
      final CodeSource cs = pd.getCodeSource();
      if (cs != null) {
        result = cs.getLocation();
      }

      if (result != null) {
        if ("file".equals(result.getProtocol())) {
          try {
            if (result.toExternalForm().endsWith(".jar") || result.toExternalForm().endsWith(".zip")) {
              result = new URL("jar:".concat(result.toExternalForm()).concat("!/").concat(clsAsResource));
            } else if (new File(result.getFile()).isDirectory()) {
              result = new URL(result, clsAsResource);
            }
          } catch (MalformedURLException ignore) {
            System.out.println("-W");
          }
        }
      }
    }

    if (result == null) {
      final ClassLoader clsLoader = cls.getClassLoader();
      result = clsLoader != null ? clsLoader.getResource(clsAsResource) : ClassLoader.getSystemResource(clsAsResource);
    }
    return result;
  }
}