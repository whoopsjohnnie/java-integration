package main.http.api;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.netty.ssl.JsseSslEngineFactory;

import com.google.gson.Gson;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * 
 * @author john.grundback
 *
 */
public class HTTPAPIClient {

	/**
	 * 
	 * @author john.grundback
	 *
	 */
	private final class OverrideX509ExtendedTrustManager extends X509ExtendedTrustManager {

		@Override
		public void checkClientTrusted(X509Certificate[] certificates, String authType) {
		}

		public void checkClientTrusted(X509Certificate[] certificates, String authType, Socket socket) {
		}

		public void checkClientTrusted(X509Certificate[] certificates, String authType, SSLEngine sslEngine) {
		}

		@Override
		public void checkServerTrusted(X509Certificate[] certificates, String authType) {
		}

		@Override
		public void checkServerTrusted(X509Certificate[] certificates, String authType, Socket socket) {
		}

		@Override
		public void checkServerTrusted(X509Certificate[] certificates, String authType, SSLEngine sslEngine) {
		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[0];
		}

	}

	public static final String AUTH_TYPE_BASIC = "basic";
	public static final String AUTH_TYPE_BEARER = "bearer";
	public static final String AUTH_TYPE_API_KEY = "apiKey";

	private static final org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager
			.getLogger(HTTPAPIClient.class);

	protected SSLContext sslContext = null;

	protected String httpClientPcol = null;
	protected String httpClientHost = null;
	protected String httpClientPort = null;
	protected String httpClientPath = null;

	protected String httpClientUrl = null;

	protected AsyncHttpClientConfig httpClientConfig = null;
	/*
	 * Don't hold on to the client, instead create a new one that we explicitly close when done with
	 */
	// protected AsyncHttpClient httpClient = null;

	// @Autowired
	// protected PropertyReader propertyReader;
	protected String httpAuthType = null;
	protected String httpAuthUser = null;
	protected String httpAuthPass = null;
	protected String httpAuthAPIKey = null;
	/*
	 * java.util.concurrent.TimeoutException
     * at java.util.concurrent.CompletableFuture.timedGet(CompletableFuture.java:1771)
     * at java.util.concurrent.CompletableFuture.get(CompletableFuture.java:1915)
     * at org.asynchttpclient.netty.NettyResponseFuture.get(NettyResponseFuture.java:207)
     * ... 
	 */
	// protected int timeout = 15000;
	protected int timeout = 60000;

	/**
	 * 
	 */
	public HTTPAPIClient() {
	}

	/**
	 * 
	 * @param sslContext
	 */
	public HTTPAPIClient(SSLContext sslContext) {
		this.sslContext = sslContext;
	}

	public void init(
		String httpPcol, 
		String httpHost, 
		String httpPort,
		String httpPath,
		String httpAuthType,
		String httpAuthUser,
		String httpAuthPass,
		String httpAuthAPIKey
	) {

		this.httpClientPcol = httpPcol;
		this.httpClientHost = httpHost;
		this.httpClientPort = httpPort;
		this.httpClientPath = httpPath;
		
		this.httpAuthType = httpAuthType;
		this.httpAuthUser = httpAuthUser;
		this.httpAuthPass = httpAuthPass;
		this.httpAuthAPIKey = httpAuthAPIKey;

		this.httpClientUrl = null;
		if( (httpPcol != null) && 
			(httpHost != null) && 
			(httpPort != null) && 
			(httpPath != null) ) {
			this.httpClientUrl = httpPcol + "://" + httpHost + ":" + httpPort + "/" + httpPath;
		}

//		if( this.sslContext != null ) {
//
//			JsseSslEngineFactory sslEngineFactory = new JsseSslEngineFactory(this.sslContext);
//
//			AsyncHttpClientConfig config = Dsl.config().setSslEngineFactory(sslEngineFactory).setConnectTimeout(this.timeout).setRequestTimeout(this.timeout).build();
//			this.httpClientConfig = config;
//
//			this.httpClient = Dsl.asyncHttpClient(
//				this.httpClientConfig
//			);
//
//		} else {
//
//			AsyncHttpClientConfig config = Dsl.config().setConnectTimeout(this.timeout).setRequestTimeout(this.timeout).build();
//			this.httpClientConfig = config;
//
//			this.httpClient = Dsl.asyncHttpClient(
//				this.httpClientConfig
//			);
//
//		}

	}

	/**
	 * 
	 * @return
	 */
	public String clientPcol() {
		return this.httpClientPcol;
	}

	/**
	 * 
	 * @return
	 */
	public String clientHost() {
		return this.httpClientHost;
	}

	/**
	 * 
	 * @return
	 */
	public String clientPort() {
		return this.httpClientPort;
	}

	/**
	 * 
	 * @return
	 */
	public String clientPath() {
		return this.httpClientPath;
	}

	/**
	 * 
	 * @return
	 */
	public String clientURL() {
		return this.httpClientUrl;
	}

	/**
	 * 
	 * @return
	 */
	public AsyncHttpClient client() {

		// if( this.httpClient != null ) {
		// 	return this.httpClient;
		// }

		// if( this.httpClient == null ) {
		// 	init();
		// }

		// return this.httpClient;

		AsyncHttpClient httpClient = null;
		if( this.sslContext != null ) {

			JsseSslEngineFactory sslEngineFactory = new JsseSslEngineFactory(this.sslContext);

			AsyncHttpClientConfig config = Dsl.config().setSslEngineFactory(sslEngineFactory).setConnectTimeout(this.timeout).setRequestTimeout(this.timeout).build();
			this.httpClientConfig = config;

			// this.
			httpClient = Dsl.asyncHttpClient(
				this.httpClientConfig
			);

		} else {

			AsyncHttpClientConfig config = Dsl.config().setConnectTimeout(this.timeout).setRequestTimeout(this.timeout).build();
			this.httpClientConfig = config;

			// this.
			httpClient = Dsl.asyncHttpClient(
				this.httpClientConfig
			);

		}

		return httpClient;
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public String buildURL(String resource, Map<String, Object> parameters) {
		log.debug(" buildURL: resource: " + resource);
		log.debug(" buildURL: parameters: ");
		log.debug(parameters);
		String url = this.clientURL() + "/" + resource;
		if( parameters != null ) {
			int i=0;
			for( String parameter : parameters.keySet() ) {
				if( i == 0 ) {
					url += "?" + parameter + "=" + parameters.get(parameter);
				} else {
					url += "&" + parameter + "=" + parameters.get(parameter);
				}
				i++;
			}
		}
		log.debug(url);
		return url;
	}

	/**
	 * 
	 * @param body
	 * @param parameters
	 * @return
	 */
	public String buildBody(String body, Map<String, Object> parameters) {
		log.debug(" buildBody: ");
		log.debug(body);
		return body;
	}

	/**
	 * 
	 * @param headers
	 * @return
	 */
	public Map<String, String> buildHeaders(Map<String, String> headers) {
		log.debug(" buildHeaders: ");
		Map<String, String> cheaders = new HashMap<String, String>();
		if(httpAuthType  != null) {
			if(HTTPAPIClient.AUTH_TYPE_BASIC.equalsIgnoreCase(httpAuthType)) {
				log.debug(" buildHeaders: Using basic auth");
				String encoded = Base64.getEncoder().encodeToString((httpAuthUser + ':' + httpAuthPass).getBytes(StandardCharsets.UTF_8));
				// request.addHeader("Authorization", "Basic " + encoded);
				cheaders.put("Authorization", "Basic " + encoded);
			} else if(HTTPAPIClient.AUTH_TYPE_BEARER.equalsIgnoreCase(httpAuthType)) {
				log.debug(" buildHeaders: Using bearer auth");
				cheaders.put("Authorization", "Bearer " + httpAuthAPIKey);
			} else if(HTTPAPIClient.AUTH_TYPE_API_KEY.equalsIgnoreCase(httpAuthType)) {
				log.debug(" buildHeaders: Using apikey auth");
				// request.addHeader("Authorization", "ApiKey " + httpAuthAPIKey);
				cheaders.put("Authorization", "ApiKey " + httpAuthAPIKey);
			}
		} else {
			log.debug("Not using any auth as its null");
		}
		log.debug(cheaders);
		return cheaders;
	}

	/**
	 * 
	 * @param request
	 * @return
	 */
	// public void executeSyncRequest(BoundRequestBuilder request) {
	public Response executeSyncRequest(BoundRequestBuilder request) {
		log.debug(" executeSyncRequest: " + request.toString());
		Future<Response> responseFuture = request.execute();
		try {
			Response response = responseFuture.get(this.timeout, TimeUnit.MILLISECONDS);
			if (response.getStatusCode() != 200) {
				log.warn(" executeSyncRequest: " + request.toString() + " responded with status: "
						+ response.getStatusCode());
			} else {
				log.debug(" executeSyncRequest: " + request.toString() + " responded with status: "
						+ response.getStatusCode());
			}
			return response;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param request
	 */
	public void executeAsyncRequest(Request request) {
		log.debug(" executeAsyncRequest: " + request.toString());
		AsyncHttpClient client = this.client();
		client.executeRequest(request, new AsyncCompletionHandler<Integer>() {
			@Override
			public Integer onCompleted(Response response) {
				if (response.getStatusCode() != 200) {
					log.warn(" executeAsyncRequest: " + request.toString() + " responded with status: "
							+ response.getStatusCode());
				} else {
					log.debug(" executeAsyncRequest: " + request.toString() + " responded with status: "
							+ response.getStatusCode());
				}
				return response.getStatusCode();
			}
		});
//		try {
//			/*
//			 * Must close here to avoid UNIX file pipe leaks
//			 */
//			client.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

	/**
	 * 
	 * @param request
	 */
	public void executeAsyncRequestWithAsyncHandler(Request request) {
		log.debug(" executeAsyncRequestWithAsyncHandler: " + request.toString());
		AsyncHttpClient client = this.client();
		client.executeRequest(request, new AsyncHandler<Integer>() {

			int responseStatusCode = -1;

			@Override
			public State onStatusReceived(HttpResponseStatus responseStatus) {
				responseStatusCode = responseStatus.getStatusCode();
				return State.CONTINUE;
			}

			@Override
			public State onHeadersReceived(HttpHeaders headers) {
				return State.CONTINUE;
			}

			@Override
			public State onBodyPartReceived(HttpResponseBodyPart bodyPart) {
				return State.CONTINUE;
			}

			@Override
			public void onThrowable(Throwable t) {

			}

			@Override
			public Integer onCompleted() {
				if (responseStatusCode != 200) {
					log.warn(" executeAsyncRequestWithAsyncHandler: " + request.toString() + " responded with status: "
							+ responseStatusCode);
				} else {
					log.debug(" executeAsyncRequestWithAsyncHandler: " + request.toString() + " responded with status: "
							+ responseStatusCode);
				}
				return responseStatusCode;
			}

		});
//		try {
//			/*
//			 * Must close here to avoid UNIX file pipe leaks
//			 */
//			client.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

	/**
	 * 
	 * @param request
	 */
	public void executeAsyncRequestWithListanableFuture(Request request) {
		log.debug(" executeAsyncRequestWithListanableFuture: " + request.toString());
		AsyncHttpClient client = this.client();
		ListenableFuture<Response> listenableFuture = client.executeRequest(request);
		listenableFuture.addListener(() -> {
			Response response;
			try {
				response = listenableFuture.get(this.timeout, TimeUnit.MILLISECONDS);
				if (response.getStatusCode() != 200) {
					log.warn(" executeAsyncRequestWithListanableFuture: " + request.toString()
							+ " responded with status: " + response.getStatusCode());
				} else {
					log.debug(" executeAsyncRequestWithListanableFuture: " + request.toString()
							+ " responded with status: " + response.getStatusCode());
				}
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
			}
		}, Executors.newCachedThreadPool());
//		try {
//			/*
//			 * Must close here to avoid UNIX file pipe leaks
//			 */
//			client.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

	/*
	 * 
	 */

	/**
	*
	* @return
	*/
//	protected Properties readPropFile() {
//		return this.propertyReader.readClientProperties();
//	}

	/**
	*
	* @param clientMPID
	* @return
	*/
//	protected Properties readPropFile(String clientMPID) {
//		return this.propertyReader.readClientProperties(clientMPID);
//	}

	/**
	*
	* @return
	*/
//	protected Properties getProperties() {
//		return this.propertyReader.getProperties();
//	}

	/**
	*
	* @param key
	* @param def
	* @return
	*/
//	protected String getProperty(String key, String def) {
//		return this.propertyReader.getProperty(key, def);
//	}

	/**
	*
	* @param key
	* @return
	*/
//	protected Object getProperty(String key) {
//		return this.propertyReader.getProperty(key);
//	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Collection<Map<String, Object>> executeIndex(String resource, String body, Map<String, Object> parameters, Map<String, String> headers) {

		log.debug(" executeIndex: resource: " + resource);
		log.debug(" executeIndex: body: ");
		log.debug(body);
		log.debug(" executeIndex: parameters: ");
		log.debug(parameters);
		log.debug(" executeIndex: headers: ");
		log.debug(headers);

		AsyncHttpClient client = this.client();
		BoundRequestBuilder request = null;
		if( body != null ) {
			request = client.prepareGet(this.buildURL(resource, parameters))
							.addHeader("Content-Type", "application/json")
							.setBody(this.buildBody(body, parameters));
		} else {
			request = client.prepareGet(this.buildURL(resource, parameters));
		}

		headers = this.buildHeaders(headers);
		if( headers != null ) {
			for( String header : headers.keySet() ) {
				request.addHeader(header, headers.get(header));
			}
		}

		Response response = this.executeSyncRequest(request);
		try {
			/*
			 * Must close here to avoid UNIX file pipe leaks
			 */
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug(" executeIndex: code: " + response.getStatusCode());
		log.debug(" executeIndex: status: " + response.getStatusText());
		log.debug(" executeIndex: body: " + response.getResponseBody());
		log.debug(response.getResponseBody());

		return this.parseAsCollection(response.getResponseBody());
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Collection<Map<String, Object>> executeIndex(String resource, Map<String, Object> parameters, Map<String, String> headers) {
		return this.executeIndex(resource, null, parameters, headers);
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @return
	 */
	public Collection<Map<String, Object>> executeIndex(String resource, Map<String, Object> parameters) {
		return this.executeIndex(resource, null, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public Collection<Map<String, Object>> executeIndex(String resource) {
		return this.executeIndex(resource, null, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executeGet(String resource, String body, Map<String, Object> parameters, Map<String, String> headers) {

		log.debug(" executeGet: resource: " + resource);
		log.debug(" executeGet: body: ");
		log.debug(body);
		log.debug(" executeGet: parameters: ");
		log.debug(parameters);
		log.debug(" executeGet: headers: ");
		log.debug(headers);

		AsyncHttpClient client = this.client();
		BoundRequestBuilder request = null;
		if( body != null ) {
			request = client.prepareGet(this.buildURL(resource, parameters))
							.addHeader("Content-Type", "application/json")
							.setBody(this.buildBody(body, parameters));
		} else {
			request = client.prepareGet(this.buildURL(resource, parameters));
		}

		headers = this.buildHeaders(headers);
		if( headers != null ) {
			for( String header : headers.keySet() ) {
				request.addHeader(header, headers.get(header));
			}
		}

		Response response = this.executeSyncRequest(request);
		try {
			/*
			 * Must close here to avoid UNIX file pipe leaks
			 */
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug(" executeGet: code: " + response.getStatusCode());
		log.debug(" executeGet: status: " + response.getStatusText());
		log.debug(" executeGet: body: " + response.getResponseBody());
		log.debug(response.getResponseBody());

		return this.parseAsRecord(response.getResponseBody());
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executeGet(String resource, Map<String, Object> parameters, Map<String, String> headers) {
		return this.executeGet(resource, null, parameters, headers);
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executeGet(String resource, Map<String, Object> parameters) {
		return this.executeGet(resource, null, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public Map<String, Object> executeGet(String resource) {
		return this.executeGet(resource, null, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executePost(String resource, String body, Map<String, Object> parameters, Map<String, String> headers) {

		log.debug(" executePost: resource: " + resource);
		log.debug(" executePost: body: ");
		log.debug(body);
		log.debug(" executePost: parameters: ");
		log.debug(parameters);
		log.debug(" executePost: headers: ");
		log.debug(headers);

		AsyncHttpClient client = this.client();
		BoundRequestBuilder request = null;
		if( body != null ) {
			request = client.preparePost(this.buildURL(resource, parameters))
							.addHeader("Content-Type", "application/json")
							.setBody(this.buildBody(body, parameters));
		} else {
			request = client.preparePost(this.buildURL(resource, parameters));
		}

		headers = this.buildHeaders(headers);
		if( headers != null ) {
			for( String header : headers.keySet() ) {
				request.addHeader(header, headers.get(header));
			}
		}

		Response response = this.executeSyncRequest(request);
		try {
			/*
			 * Must close here to avoid UNIX file pipe leaks
			 */
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug(" executePost: code: " + response.getStatusCode());
		log.debug(" executePost: status: " + response.getStatusText());
		log.debug(" executePost: body: " + response.getResponseBody());
		log.debug(response.getResponseBody());

		return this.parseAsRecord(response.getResponseBody());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executePost(String resource, String body, Map<String, Object> parameters) {
		return this.executePost(resource, body, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @return
	 */
	public Map<String, Object> executePost(String resource, String body) {
		return this.executePost(resource, body, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executePost(String resource, Map<String, Object> parameters, Map<String, String> headers) {
		return this.executePost(resource, null, parameters, headers);
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executePost(String resource, Map<String, Object> parameters) {
		return this.executePost(resource, null, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public Map<String, Object> executePost(String resource) {
		return this.executePost(resource, null, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executePut(String resource, String body, Map<String, Object> parameters, Map<String, String> headers) {

		log.debug(" executePut: resource: " + resource);
		log.debug(" executePut: body: ");
		log.debug(body);
		log.debug(" executePut: parameters: ");
		log.debug(parameters);
		log.debug(" executePut: headers: ");
		log.debug(headers);

		AsyncHttpClient client = this.client();
		BoundRequestBuilder request = null;
		if( body != null ) {
			request = client.preparePut(this.buildURL(resource, parameters))
							.addHeader("Content-Type", "application/json")
							.setBody(this.buildBody(body, parameters));
		} else {
			request = client.preparePut(this.buildURL(resource, parameters));
		}
		
		headers = this.buildHeaders(headers);
		if( headers != null ) {
			for( String header : headers.keySet() ) {
				request.addHeader(header, headers.get(header));
			}
		}

		Response response = this.executeSyncRequest(request);
		try {
			/*
			 * Must close here to avoid UNIX file pipe leaks
			 */
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug(" executeUpdate: code: " + response.getStatusCode());
		log.debug(" executeUpdate: status: " + response.getStatusText());
		log.debug(" executeUpdate: body: " + response.getResponseBody());
		log.debug(response.getResponseBody());

		return this.parseAsRecord(response.getResponseBody());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executePut(String resource, String body, Map<String, Object> parameters) {
		return this.executePut(resource, body, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @return
	 */
	public Map<String, Object> executePut(String resource, String body) {
		return this.executePut(resource, body, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executePut(String resource, Map<String, Object> parameters, Map<String, String> headers) {
		return this.executePut(resource, null, parameters, headers);
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executePut(String resource, Map<String, Object> parameters) {
		return this.executePut(resource, null, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public Map<String, Object> executePut(String resource) {
		return this.executePut(resource, null, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @param body
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executeDelete(String resource, String body, Map<String, Object> parameters, Map<String, String> headers) {

		log.debug(" executeDelete: resource: " + resource);
		log.debug(" executeDelete: body: ");
		log.debug(body);
		log.debug(" executeDelete: parameters: ");
		log.debug(parameters);
		log.debug(" executeDelete: headers: ");
		log.debug(headers);

		AsyncHttpClient client = this.client();
		BoundRequestBuilder request = null;
		if( body != null ) {
			request = client.prepareDelete(this.buildURL(resource, parameters))
							.addHeader("Content-Type", "application/json")
							.setBody(this.buildBody(body, parameters));
		} else {
			request = client.prepareDelete(this.buildURL(resource, parameters));
		}

		headers = this.buildHeaders(headers);
		if( headers != null ) {
			for( String header : headers.keySet() ) {
				request.addHeader(header, headers.get(header));
			}
		}

		Response response = this.executeSyncRequest(request);
		try {
			/*
			 * Must close here to avoid UNIX file pipe leaks
			 */
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug(" executeDelete: code: " + response.getStatusCode());
		log.debug(" executeDelete: status: " + response.getStatusText());
		log.debug(" executeDelete: body: " + response.getResponseBody());
		log.debug(response.getResponseBody());

		return this.parseAsRecord(response.getResponseBody());
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @param headers
	 * @return
	 */
	public Map<String, Object> executeDelete(String resource, Map<String, Object> parameters, Map<String, String> headers) {
		return this.executeDelete(resource, null, parameters, headers);
	}

	/**
	 * 
	 * @param resource
	 * @param parameters
	 * @return
	 */
	public Map<String, Object> executeDelete(String resource, Map<String, Object> parameters) {
		return this.executeDelete(resource, null, parameters, new HashMap<String, String>());
	}

	/**
	 * 
	 * @param resource
	 * @return
	 */
	public Map<String, Object> executeDelete(String resource) {
		return this.executeDelete(resource, null, new HashMap<String, Object>(), new HashMap<String, String>());
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	public Collection<Map<String, Object>> parseAsCollection(String response) {
		Gson gson = new Gson();
		Collection<Map<String, Object>> resp = (Collection<Map<String, Object>>)gson.fromJson(response, Map.class);
		return resp;
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	public Map<String, Object> parseAsRecord(String response) {
		Gson gson = new Gson();
		Map<String, Object> resp = (Map<String, Object>)gson.fromJson(response, Map.class);
		return resp;
	}

}
