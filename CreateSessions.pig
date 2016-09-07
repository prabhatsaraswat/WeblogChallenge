register datafu-pig-incubating-1.3.1.jar
register piggybank-0.11.0.jar

DEFINE ConvTimeToUnix   org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE BreakSessions  datafu.pig.sessions.Sessionize('30m');
DEFINE Quantile    datafu.pig.stats.StreamingQuantile('0.75','0.90','0.95','1');
DEFINE Median      datafu.pig.stats.Median();

logs = LOAD '/weblogchallenge/2015_07_22_mktplace_shop_web_log_sample.log' USING  org.apache.pig.piggybank.storage.CSVExcelStorage(' ') AS ( ts:chararray,  elb:chararray, ip_port:chararray, server_port:chararray, request_processing_time:float, backend_processing_time:float, response_processing_time:float, elb_status_code:int,backend_status_code:int,received_bytes:int,sent_bytes:int,request:chararray,ua:chararray,ssl_cipher:chararray,ssl_protocol:chararray);
 
required_logs = FOREACH logs GENERATE ConvTimeToUnix(ts) as isoTime,
              ts,
              flatten(STRSPLIT(ip_port, ':')) as (ip:chararray, port:chararray),
              flatten(STRSPLIT(request, 'HTTP/')) as (url:chararray, v:chararray);

visitor_level_required_logs = GROUP required_logs BY ip;
SessionizedLogs = FOREACH visitor_level_required_logs {
       ordered = ORDER required_logs BY isoTime ;
      GENERATE FLATTEN(BreakSessions(ordered)) AS (isoTime,ts, ip,port,url,v, s_id);
 };
session_times = FOREACH (GROUP SessionizedLogs BY s_id){
				url = DISTINCT SessionizedLogs.url;
				ip = DISTINCT SessionizedLogs.ip;
				GENERATE group as s_id,
                         (MAX(SessionizedLogs.isoTime)-MIN(SessionizedLogs.isoTime))
                            / 1000.0 / 60.0 as session_length,
                            FLATTEN(ip), COUNT(url) as unique_urls;
}

dump session_times;

session_times_sum = FOREACH (GROUP session_times BY ip){
	total_session_time = SUM(session_times.session_length);
	GENERATE FLATTEN(session_times.ip) as ip, total_session_time as total_session_time;
}
session_times_sum =  DISTINCT session_times_sum;

ordered_session_time_sum = ORDER session_times_sum BY total_session_time DESC;
most_engaged_users = LIMIT ordered_session_time_sum 10;
dump most_engaged_users;

# (220.226.206.7,145.90448333333336)
# (52.74.219.71,118.04858333333333)
# (119.81.61.166,117.93630000000002)
# (54.251.151.39,117.71238333333332)
# (121.58.175.128,114.37013333333333)
# (106.186.23.95,113.68785)
# (125.19.44.66,108.57385)
# (54.244.52.204,108.17736666666667)
# (54.169.191.85,107.64415)
# (207.46.13.22,106.92846666666667)

session_summary = FOREACH (GROUP session_times ALL) {
  ordered = ORDER session_times BY session_length;
  GENERATE
    AVG(ordered.session_length) as avg_session,
    Median(ordered.session_length) as median_session,
    Quantile(ordered.session_length) as quantiles_session;
};
session_summary = LIMIT session_summary 10;

dump session_summary;
                
				
# (2.7338191444763953,(0.35689166666666666),(1.91685,6.313149999999999,18.849716666666666,54.13606666666667))
