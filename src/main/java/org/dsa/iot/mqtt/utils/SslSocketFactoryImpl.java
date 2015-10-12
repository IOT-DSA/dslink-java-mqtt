package org.dsa.iot.mqtt.utils;

import io.netty.util.CharsetUtil;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * @author Samuel Grenier
 */
/**
 * @author Samuel Grenier
 */
public class SslSocketFactoryImpl extends SSLSocketFactory {

    private final SSLContext context;

    public SslSocketFactoryImpl(String caCert,
                                String sslCert,
                                String sslPrivKey) {
        try {
            KeyManagerFactory kmf = null;
            TrustManagerFactory tmf = null;
            if (sslCert != null) {
                X509CertificateHolder holder = (X509CertificateHolder) read(caCert);
                Certificate root = holderToCert(holder);

                holder = (X509CertificateHolder) read(sslCert);
                Certificate certificate = holderToCert(holder);

                // Initialize CA cert
                String def = KeyStore.getDefaultType();
                KeyStore caKs = KeyStore.getInstance(def);
                caKs.load(null, null);
                caKs.setCertificateEntry("root", root);

                // Initialize client cert
                KeyStore store = KeyStore.getInstance(def);
                store.load(null);
                store.setCertificateEntry("crt", certificate);
                Certificate[] certs = new Certificate[] {
                        certificate
                };

                PEMKeyPair pem = (PEMKeyPair) read(sslPrivKey);
                KeyPair pair = pemToPair(pem);
                store.setKeyEntry("pk", pair.getPrivate(), new char[0], certs);

                // Initialize factories
                def = TrustManagerFactory.getDefaultAlgorithm();
                tmf = TrustManagerFactory.getInstance(def);
                tmf.init(caKs);

                def = KeyManagerFactory.getDefaultAlgorithm();
                kmf = KeyManagerFactory.getInstance(def);
                kmf.init(store, new char[0]);
            }

            context = SSLContext.getInstance("TLS");

            KeyManager[] km = null;
            if (kmf != null) {
                km = kmf.getKeyManagers();
            }

            TrustManager[] tm;
            if (tmf == null) {
                tm = new TrustManager[] {
                        new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] c,
                                                       String a) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] c,
                                                       String a) {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                    }
                };
            } else {
                tm = tmf.getTrustManagers();
            }

            context.init(km, tm, null);
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
    public Socket createSocket(InetAddress a, int i, InetAddress a2, int i1)
                                                            throws IOException {
        return context.getSocketFactory().createSocket(a, i, a2, i1);
    }

    public static KeyPair pemToPair(PEMKeyPair pair) throws PEMException {
        JcaPEMKeyConverter c = new JcaPEMKeyConverter();
        c.setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return c.getKeyPair(pair);
    }

    public static X509Certificate holderToCert(X509CertificateHolder holder)
                                            throws CertificateException {
        JcaX509CertificateConverter c = new JcaX509CertificateConverter();
        c.setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return c.getCertificate(holder);
    }

    public static Object read(String string) throws IOException {
        PEMParser parser;
        {
            byte[] bytes = string.getBytes(CharsetUtil.UTF_8);
            InputStream in = new ByteArrayInputStream(bytes);
            InputStreamReader isr = new InputStreamReader(in, CharsetUtil.UTF_8);
            parser = new PEMParser(isr);
        }

        Object obj = parser.readObject();
        parser.close();
        return obj;
    }

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
}
