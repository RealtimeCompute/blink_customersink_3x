package com.alibaba.blink.customersink;


import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RedisSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private List<Row> cache = new ArrayList<Row>();
    private int batchSize;
    private int db;
    private String host;
    private int port;
    private String password;
    private static volatile JedisPool pool;
    private static int refCount;

    public void open(int taskNumber, int numTasks) throws IOException{
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
        String[] filedNames = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        LOG.info(String.format("Open Method Called: filedNames %d typeInformations %d", filedNames.length,
                typeInformations.length));

        host = userParamsMap.get("host");
        port = Integer.valueOf(userParamsMap.get("port"));
        password = userParamsMap.get("password");
        batchSize = userParamsMap.containsKey("batchsize")?Integer.valueOf(userParamsMap.get("batchsize")) : 1;
        db = userParamsMap.containsKey("db")?Integer.valueOf(userParamsMap.get("db")) : 0;

        synchronized (RedisSink.class) {
            if (pool == null) {
                pool = createJedisPool();
            }
            refCount++;
        }
    }

    public void close() throws IOException{
        synchronized (RedisSink.class) {
            refCount--;
            if (refCount <= 0 && pool != null) {
                pool.close();
                pool = null;
            }
        }
    }

    public void writeAddRecord(Row row) throws IOException {
        String key = row.getField(0).toString();
        String value = row.getField(1).toString();
        LOG.info("key:"+key+",value:"+value);
        try{
            cache.add(row);
            if (cache.size() >= batchSize) {
                try (Jedis jedis = pool.getResource()) {
                    for(int i = 0; i < cache.size(); i ++) {
                        jedis.set(cache.get(i).getField(0).toString(), cache.get(i).getField(1).toString());
                    }
                }
                cache.clear();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void writeDeleteRecord(Row row) throws IOException{
        String key = row.getField(0).toString();
        try(Jedis jedis = pool.getResource()){
            jedis.del(key);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sync() throws IOException{
        if(cache.size()>0) {
            try (Jedis jedis = pool.getResource()) {
                for(int i = 0; i < cache.size(); i ++) {
                    jedis.set(cache.get(i).getField(0).toString(), cache.get(i).getField(1).toString());
                }
                cache.clear();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public String getName() {
        return "RedisSink";
    }

    protected JedisPool createJedisPool() {
        String pass = StringUtils.isNullOrWhitespaceOnly(password) ? null : password;
        return new JedisPool(new JedisPoolConfig(), host, port, 3000, pass, db);
    }
}