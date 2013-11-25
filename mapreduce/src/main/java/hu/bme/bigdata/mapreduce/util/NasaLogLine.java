package hu.bme.bigdata.mapreduce.util;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.hadoop.io.Text;

@Getter
@Setter
@ToString
public class NasaLogLine {
	
	private static final Pattern PATTERN = Pattern.compile("^([^ ]+)( - - \\[(.+)\\] \"([^ ]+) ([^ ]*) .*)?");
	
	private final String host;
	
	private String request;
	
	private String replyCode;
	
	private Date timestamp;
	
	private String requestType;
	
	public NasaLogLine(Text inputLine) {
		String inputLinString = inputLine.toString();
		Matcher matcher = PATTERN.matcher(inputLinString);
		System.out.println(matcher.matches());
		host = matcher.group(1);
		String timeString = matcher.group(3);
		request = matcher.group(5);
		requestType = matcher.group(4);
		
	}
	
	
	public static void main(String[] args) {
//		Text text = new Text("234.213.213.322");
		Text text = new Text("ix-mem-tn2-09.ix.netcom.com - - [27/Aug/1995:12:09:14 -0400] \"HEAD / HTTP/1.0\" 200 0");

		NasaLogLine nasaLogLine = new NasaLogLine(text);
		System.out.println(nasaLogLine);
	}
}
