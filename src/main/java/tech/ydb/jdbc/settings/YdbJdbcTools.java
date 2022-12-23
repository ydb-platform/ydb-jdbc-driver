package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.YdbConfigurationException;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbJdbcTools {
    private static final Logger logger = LoggerFactory.getLogger(YdbJdbcTools.class);

    private YdbJdbcTools() { }

    private static SQLException invalidFormatException() {
        return new SQLException("URL must be provided in form " + YdbConst.JDBC_YDB_PREFIX +
                "host:port[/database][?param1=value1&paramN=valueN]");
    }

    //
    public static boolean isYdb(String url) {
        return url.startsWith(YdbConst.JDBC_YDB_PREFIX);
    }

    @SuppressWarnings("UnstableApiUsage")
    public static YdbProperties from(String url, Properties origProperties) throws SQLException {
        if (!isYdb(url)) {
            String msg = "[" + url + "] is not a YDB URL, must starts from " + YdbConst.JDBC_YDB_PREFIX;
            throw new YdbConfigurationException(msg);
        }

        try {
            url = url.substring(YdbConst.JDBC_YDB_PREFIX.length());

            String addressesUrl = getAddressesUrl(url);
            url = url.substring(addressesUrl.length());

            List<HostAndPort> addresses = getAddresses(addressesUrl);
            if (addresses.size() > 1) {
                throw new SQLException("Multiple hosts are not supported");
            }

            Properties properties = withQuery(url, origProperties, getDatabase(url));
            YdbConnectionProperties connectionProperties = new YdbConnectionProperties(addresses.get(0),
                    parseProperties(properties, YdbConnectionProperty.properties()));

            YdbClientProperties ydbClientProperties = new YdbClientProperties(
                    parseProperties(properties, YdbClientProperty.properties()));
            YdbOperationProperties ydbOperationProperties = new YdbOperationProperties(
                    parseProperties(properties, YdbOperationProperty.properties()));
            return new YdbProperties(connectionProperties, ydbClientProperties, ydbOperationProperties);
        } catch (RuntimeException ex) {
            throw new YdbConfigurationException(ex.getMessage(), ex);
        }
    }

    //

    private static String getAddressesUrl(String url) {
        int databaseSep = url.indexOf('/');
        int paramsSep = url.indexOf('?');

        boolean nextDatabase = databaseSep >= 0;
        boolean nextParams = paramsSep >= 0;

        if (nextDatabase || nextParams) {
            int pos;
            if (nextDatabase && nextParams) {
                pos = Math.min(databaseSep, paramsSep);
            } else if (nextDatabase) {
                pos = databaseSep;
            } else {
                pos = paramsSep;
            }
            return url.substring(0, pos);
        } else {
            return url;
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private static List<HostAndPort> getAddresses(String part) throws SQLException {
        String[] addresses = part.split(",");
        if (addresses.length == 0) {
            throw invalidFormatException();
        }
        return Stream.of(addresses)
                .map(HostAndPort::fromString)
                .collect(Collectors.toList());
    }

    @Nullable
    private static String getDatabase(String url) {
        String database;
        int paramsSeparator = url.indexOf('?');
        if (paramsSeparator == 0) {
            return null;
        } else if (paramsSeparator > 0) {
            database = url.substring(0, paramsSeparator);
        } else {
            database = url;
        }
        if (database.isEmpty() || database.equals("/")) {
            return null;
        }
        return database;
    }

    private static Properties withQuery(String url, Properties defaults, String database) {
        Properties properties = new Properties();
        properties.putAll(defaults);
        if (database != null) {
            properties.setProperty(YdbConnectionProperty.DATABASE.getName(), database);
        }
        int paramsSeparator = url.indexOf('?');
        if (paramsSeparator < 0) {
            return properties;
        }

        String params = url.substring(paramsSeparator + 1);
        if (params.isEmpty()) {
            return properties;
        }
        String[] kv = params.split("&");
        for (String keyValue : kv) {
            String[] tokens = keyValue.split("=", 2);
            if (tokens.length == 2) {
                properties.put(tokens[0], tokens[1]);
            } else {
                logger.error("Invalid property: {}", keyValue);
            }
        }
        return properties;
    }

    private static <T extends AbstractYdbProperty<?, ?>> Map<T, ParsedProperty> parseProperties(
            Properties properties,
            Collection<T> knownProperties) throws SQLException {
        Map<T, ParsedProperty> result = new LinkedHashMap<>(knownProperties.size());
        for (T property : knownProperties) {
            String title = property.getName();
            Object value = properties.get(title);

            PropertyConverter<?> converter = property.getConverter();
            ParsedProperty parsed;
            if (value != null) {
                if (value instanceof String) {
                    String stringValue = (String) value;
                    try {
                        parsed = new ParsedProperty(stringValue, converter.convert(stringValue));
                    } catch (SQLException e) {
                        throw new YdbConfigurationException("Unable to convert property " +
                                title + ": " + e.getMessage(), e);
                    }
                } else {
                    if (property.getType().isAssignableFrom(value.getClass())) {
                        parsed = new ParsedProperty("", value);
                    } else {
                        throw new SQLException("Invalid object property " + title +
                                ", must be " + property.getType() + ", got " + value.getClass());
                    }
                }
            } else {
                String stringValue = property.getDefaultValue();
                if (stringValue != null) {
                    try {
                        parsed = new ParsedProperty(stringValue, converter.convert(stringValue));
                    } catch (SQLException e) {
                        throw new YdbConfigurationException("Unable to convert property " +
                                title + ": " + e.getMessage(), e);
                    }
                } else {
                    parsed = null;
                }
            }

            result.put(property, parsed);
        }
        return Collections.unmodifiableMap(result);
    }


}
