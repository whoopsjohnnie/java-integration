package main.http.api;

/**
 * 
 * @author john.grundback
 *
 */
public class HTTPAPIClientException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3468523565416778108L;

	/**
	 * 
	 * @param message
	 */
	public HTTPAPIClientException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 */
	public HTTPAPIClientException(String message, Exception cause) {
		super(message, cause);
	}

	/**
	 * 
	 * @param statusCode
	 * @param statusText
	 * @param responseBody
	 */
	public HTTPAPIClientException(int statusCode, String statusText, String responseBody) {
		super(statusCode + ", " + statusText + ": " + responseBody);
	}

}
