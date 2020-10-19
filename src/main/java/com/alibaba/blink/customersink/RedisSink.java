package com.alibaba.blink.customersink;


import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RedisSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private Jedis jedis;
    private List<Row> cache = new ArrayList<Row>();
    private int batchsize;

    public void open(int taskNumber, int numTasks) throws IOException{
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
        String[] filedNames = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        LOG.info(String.format("Open Method Called: filedNames %d typeInformations %d", filedNames.length,
                typeInformations.length));

        String host = userParamsMap.get("host");
        int port = Integer.valueOf(userParamsMap.get("port"));
        String password = userParamsMap.get("password");
        batchsize = userParamsMap.containsKey("batchsize")?Integer.valueOf(userParamsMap.get("batchsize")):1;
        try {
            jedis = new Jedis(host,port);
            jedis.auth(password);
            if (!jedis.auth(password).equals("OK"))
            {
                LOG.error("AUTH Failed, exit!");
                return;
            }
            if(userParamsMap.containsKey("db")){
                jedis.select(Integer.valueOf(userParamsMap.get("db")));
            }else {
                jedis.select(0);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException{
        try {
            jedis.quit();
            jedis.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void writeAddRecord(Row row) throws IOException {
        //String key = row.getField(0).toString();
        //String value = row.getField(1).toString();
        //LOG.info("key:"+key+",value:"+value);
        try{
            cache.add(row);
            if (cache.size() >= batchsize) {
                for(int i = 0; i < cache.size(); i ++) {
                    Row cachedRow = cache.get(i);
                    jedis.set(cachedRow.getField(0).toString(), cachedRow.getField(1).toString());
                }
                cache.clear();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void writeDeleteRecord(Row row) throws IOException{
        String key = row.getField(0).toString();
        try{
            jedis.del(key);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sync() throws IOException{
        if(cache.size()>0) {
            try {
                for(int i = 0; i < cache.size(); i ++) {
                    String key = cache.get(i).getField(0).toString();
                    String value = cache.get(i).getField(1).toString();
                    jedis.set(key, value);
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
}

