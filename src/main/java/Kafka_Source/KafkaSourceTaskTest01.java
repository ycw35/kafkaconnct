package Kafka_Source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.cs.StandardCharsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaSourceTaskTest01 extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceTest01.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;
    private int batchSize = KafkaSourceTest01.DEFAULT_TASK_BATCH_SIZE;

    private Long streamOffset;

    public String version() {
        return new KafkaSourceTest01().version();
    }

    public void start(Map<String, String> map) {
        filename = map.get(KafkaSourceTest01.FILE_CONFIG);
        if(filename==null || filename.isEmpty()){
            stream = System.in;
            //还需要指定编码格式 utf_8
            reader = new BufferedReader(new InputStreamReader(stream));
        }
        topic = map.get(KafkaSourceTest01.TOPIC_CONFIG);
        batchSize = Integer.parseInt(map.get(KafkaSourceTest01.TASK_BATCH_SIZE_CONFIG));
    }

    //返回了 records：List<SourceRecord> 数据集
    public List<SourceRecord> poll() throws InterruptedException {
        if( stream ==null){
            try{
                stream = Files.newInputStream(Paths.get(filename));
                //?? Map<String,Object> offset  应该是获取offset的信息
                Map<String,Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD,filename));
                if (offset !=null){
                    Object lastRecordedOffset =offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)){
                        throw new ConnectException("Offset position is the incorrect type");
                    }
                    if (lastRecordedOffset != null){
                        log.debug("Found previous offset,trying to skip to file offser{}",lastRecordedOffset);
                        Long skipLeft = (Long)lastRecordedOffset;
//??
                        while(skipLeft>0){
                            try{
                                Long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            }catch (IOException e){
                                log.error("Error while trying to seek to previous offset in file {}: ", filename, e);
                                throw new ConnectException();
                            }
                        }
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                    streamOffset = (lastRecordedOffset != null) ? (long)lastRecordedOffset : 0L;
                }else {
                    streamOffset = 0L;
                }
                //还需要设置编码格式 UTF_8
                reader = new BufferedReader(new InputStreamReader(stream));
                //log.debug("Opened {} for reading", logFilename());
                log.debug("Opened {} for reading");
            }catch (NoSuchFileException e){
                log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
                synchronized (this) {
                    this.wait(1000);
                }
                return null;
            }catch (IOException e){
                log.error("Error while trying to open file {}: ", filename, e);
                //throw new ConnectException(e);
            }
        }
        try{
            final BufferedReader readerCopy;
            synchronized (this){
                readerCopy = reader;
            }
            if (readerCopy == null) return null;
            //返回值 records:应该由源数据组成的数组列表
            ArrayList<SourceRecord> records = null;

            int nread = 0;
            while(readerCopy.ready()){
                nread = readerCopy.read(buffer,offset,buffer.length-offset);
                log.trace("Read {} from{}",nread,logFilename());

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length*2];
                        System.arraycopy(buffer,0,newbuf,0,buffer.length);
                        buffer = newbuf ;
                    }
                    String line ;
                    do{
                        line = extractLine() ;
                        if ( line != null) {
                            log.trace("Read a line from {}",logFilename());
                            if (records == null ) {
                                records = new ArrayList<>() ;
                                records.add (new SourceRecord(offsetKey(filename),offsetValue(streamOffset),topic,null,null,null,VALUE_SCHEMA,line,System.currentTimeMillis()));

                                if (records.size() >= batchSize) {
                                    return records;
                                }
                            }
                        }
                    } while (line != null);
                }
            }
            if (nread < 0) {
                synchronized (this) {
                    this.wait(1000);
                }
            }
            return records ;
        }catch (IOException e){

        }
        return null;
    }

    //
    private String extractLine() {
        int util = -1 , newStart = -1 ;
        for (int i = 0; i <offset ; i++) {
            if (buffer[i] == '\n' ) {
                util = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                if (i+1>offset) return null;
                util = i;
                newStart = (buffer[i] == '\n')? i+2 : i+1;
                break;
            }
        }

        if (util != -1) {
            String result = new String(buffer, 0, util);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)  streamOffset += newStart;
            return result;
        } else {
                return null;
        }
    }

    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            this.notify();
        }
    }
    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }
}
