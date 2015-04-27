package org.dsa.iot.mqtt.utils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.cert.X509Certificate;

/**
 * @author Samuel Grenier
 */
/**
 * @author Samuel Grenier
 */
public class InsecureSslSocketFactory extends SSLSocketFactory {

    private final SSLContext context;

    public InsecureSslSocketFactory() {
        try {
            context = SSLContext.getInstance("TLS");

            TrustManager tm = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };

            context.init(null, new TrustManager[] { tm }, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return context.getSocketFactory().getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return context.getSocketFactory().getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
        return context.getSocketFactory().createSocket();
    }

    @Override
    public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
        return context.getSocketFactory().createSocket(socket, s, i, b);
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException {
        return context.getSocketFactory().createSocket(s, i);
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress a, int i2) throws IOException {
        return context.getSocketFactory().createSocket(s, i, a, i2);
    }

    @Override
    public Socket createSocket(InetAddress a, int i) throws IOException {
        return context.getSocketFactory().createSocket(a, i);
    }

    @Override
    public Socket createSocket(InetAddress a, int i, InetAddress a2, int i1) throws IOException {
        return context.getSocketFactory().createSocket(a, i, a2, i1);
    }
}
