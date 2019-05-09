package org.gy.framework.redis.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private static final int HTTP_RESPONSE_CODE_FLAG = 300;

    public static final String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";

    public static final String CONTENT_TYPE_XML = "text/xml; charset=UTF-8";

    private static final String HTTP_METHOD_POST = "POST";

    private static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";

    private static final String HTTP_HEADER_CONTENT_LENGTH = "Content-Length";
    
    private static final String USER_AGENT = "User-Agent";
    
    private static final String FAKE_USER_AGENT = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)";

    /**
     * Contacts the remote URL and returns the response.
     *
     * @return the response.
     */
    public static String getResponseFromServer(final String url, int connectionTimeout, int readTimeout,
                                               boolean trustAllCerts) throws IOException {
        URLConnection conn = new URL(url).openConnection(Proxy.NO_PROXY);
        conn.setRequestProperty(USER_AGENT, FAKE_USER_AGENT);
        initSslSetting(conn, trustAllCerts);
        if (connectionTimeout >= 0) {
            conn.setConnectTimeout(connectionTimeout);
        }
        if (readTimeout >= 0) {
            conn.setReadTimeout(readTimeout);
        }
        if (conn instanceof HttpURLConnection) {
            validateResponse((HttpURLConnection) conn);
        }
        return readResult(conn.getInputStream());
    }

    private static void initSslSetting(URLConnection conn, boolean trustAllCerts) {
        if (conn instanceof HttpsURLConnection) {
            if (trustAllCerts) {
                try {
                    SSLContext sslCtx = SSLContext.getInstance("TLS");
                    sslCtx.init(null, new TrustManager[]{new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] chain, String authType)
                                throws CertificateException {
                            // do nothing
                        }

                        public void checkServerTrusted(X509Certificate[] chain, String authType)
                                throws CertificateException {
                            // do nothing
                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                    }}, new SecureRandom());
                    ((HttpsURLConnection) conn).setSSLSocketFactory(sslCtx.getSocketFactory());
                    ((HttpsURLConnection) conn).setHostnameVerifier(new HostnameVerifier() {
                        public boolean verify(String s, SSLSession sslSession) {
                            return true;
                        }
                    });
                } catch (Exception ex) {
                    logger.warn("Error when set sslCtx.", ex);
                }
            } else {
                ((HttpsURLConnection) conn).setHostnameVerifier(HttpsURLConnection.getDefaultHostnameVerifier());
            }
        }
    }

    public static String getResponseViaPost(final String url, String param, String contentType,
                                            int connectionTimeout, int readTimeout,
                                            boolean trustAllCerts) throws IOException {
        URLConnection connection = new URL(url).openConnection(Proxy.NO_PROXY);
        initSslSetting(connection, trustAllCerts);
        HttpURLConnection con = (HttpURLConnection) connection;
        if (connectionTimeout >= 0) {
            con.setConnectTimeout(connectionTimeout);
        }
        if (readTimeout >= 0) {
            con.setReadTimeout(readTimeout);
        }
        con.setInstanceFollowRedirects(true);
        con.setDoOutput(true);
        con.setRequestMethod(HTTP_METHOD_POST);
        con.setDoInput(true);
        con.setUseCaches(false);
        con.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
        con.setRequestProperty(HTTP_HEADER_CONTENT_LENGTH,
                Integer.toString(param.getBytes(Charset.defaultCharset()).length));
        con.setRequestProperty(USER_AGENT, FAKE_USER_AGENT);
        final DataOutputStream printout = new DataOutputStream(con.getOutputStream());
        printout.write(param.getBytes("UTF-8"));
        printout.flush();
        printout.close();
        validateResponse(con);
        return readResult(con.getInputStream());
    }

    private static void validateResponse(HttpURLConnection con) throws IOException {
        if (con.getResponseCode() >= HTTP_RESPONSE_CODE_FLAG) {
            try {
                InputStream es = con.getErrorStream();
                if (es != null) {
                    logger.warn("Did not receive successful HTTP response," + "response content is "
                            + readResult(es));
                }
            } catch (Exception ex) {
                logger.warn("Exception occur when process errorStream", ex);
            }
            throw new IOException("Did not receive successful HTTP response: status code = " + con.getResponseCode()
                    + ", status message = [" + con.getResponseMessage() + "]");
        }
    }

    private static String readResult(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        try {
            StringBuilder temp = new StringBuilder();
            String line = reader.readLine();
            while (line != null) {
                temp.append(line);
                line = reader.readLine();
            }
            return temp.toString();
        } finally {
            reader.close();
        }
    }
}
