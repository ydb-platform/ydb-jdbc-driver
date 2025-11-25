package tech.ydb.jdbc.context;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Collections;
import java.util.Enumeration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tech.ydb.jdbc.spi.YdbQueryExtentionService;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QuerySpiTestLoader extends URLClassLoader {
    private final static Class<?> SPI = YdbQueryExtentionService.class;
    private Class<?>[] classes;

    public QuerySpiTestLoader(ClassLoader prev, Class<?>... implementingClasses) {
        super(new URL[0], prev);
        this.classes = implementingClasses;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (name.equals("META-INF/services/" + SPI.getName())) {
            if (classes == null) {
                return Collections.emptyEnumeration();
            }
            URL url = new URL("mock", "junit", 1234, "/service", new URLStreamHandler() {
                @Override
                protected URLConnection openConnection(URL u) {
                return new URLConnection(u) {
                    @Override
                    public void connect() { }

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new ByteArrayInputStream(Stream.of(classes)
                                .map(Class::getName)
                                .collect(Collectors.joining("\n"))
                                .getBytes());
                    }
                };
                }
            });

            return new Enumeration<URL>() {
                boolean hasNext = true;

                @Override
                public boolean hasMoreElements() {
                    return hasNext;
                }

                @Override
                public URL nextElement() {
                    hasNext = false;
                    return url;
                }
            };
        }
        return super.getResources(name);
    }
}