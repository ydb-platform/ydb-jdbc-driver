package tech.ydb.jdbc.settings;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import tech.ydb.core.utils.URITools;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.YdbConfigurationException;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbJdbcTools {
    private YdbJdbcTools() { }

    public static boolean isYdb(String url) {
        return url.startsWith(YdbConst.JDBC_YDB_PREFIX);
    }

    public static YdbProperties from(String jdbcURL, Properties origProperties) throws SQLException {
        if (!isYdb(jdbcURL)) {
            String msg = "[" + jdbcURL + "] is not a YDB URL, must starts from " + YdbConst.JDBC_YDB_PREFIX;
            throw new YdbConfigurationException(msg);
        }

        try {
            String ydbURL = jdbcURL.substring(YdbConst.JDBC_YDB_PREFIX.length());
            String connectionString = ydbURL;
            String safeURL = ydbURL;

            Properties properties = new Properties();
            properties.putAll(origProperties);

            if (!ydbURL.isEmpty()) {
                URI url = new URI(ydbURL.contains("://") ? ydbURL : "grpc://" + ydbURL);
                Map<String, List<String>> params = URITools.splitQuery(url);

                // merge properties and query params
                params.forEach((key, list) -> properties.put(key, list.get(list.size() - 1)));

                StringBuilder sb = new StringBuilder();
                sb.append(url.getScheme()).append("://");
                sb.append(url.getHost());
                if (url.getPort() > 0) {
                    sb.append(":").append(url.getPort());
                }
                sb.append(url.getPath());

                connectionString = sb.toString();

                if (!params.isEmpty()) {
                    String prefix = "?";
                    for (Map.Entry<String, List<String>> entry: params.entrySet()) {
                        String value = entry.getValue().get(entry.getValue().size() - 1);
                        if (YdbConnectionProperty.TOKEN.getName().equalsIgnoreCase(entry.getKey())) {
                            value = "***";
                        }
                        if (value != null && !value.isEmpty()) {
                            sb.append(prefix);
                            sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                            sb.append("=");
                            sb.append(URLEncoder.encode(value, StandardCharsets.UTF_8));
                            prefix = "&";
                        }
                    }
                }

                safeURL = sb.toString();
            }

            YdbConnectionProperties ydbConnectionProps = new YdbConnectionProperties(
                    safeURL, connectionString, parseProperties(properties, YdbConnectionProperty.properties()));
            YdbClientProperties ydbClientProperties = new YdbClientProperties(
                    parseProperties(properties, YdbClientProperty.properties()));
            YdbOperationProperties ydbOperationProperties = new YdbOperationProperties(
                    parseProperties(properties, YdbOperationProperty.properties()));

            return new YdbProperties(ydbConnectionProps, ydbClientProperties, ydbOperationProperties);
        } catch (URISyntaxException | RuntimeException ex) {
            throw new YdbConfigurationException(ex.getMessage(), ex);
        }
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
