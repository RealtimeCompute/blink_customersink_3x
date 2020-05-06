package com.alibaba.blink.customersink;

import com.alibaba.blink.monitor.LogProducerProvider;
import com.alibaba.blink.monitor.MetricMessage;
import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PrintCustomSink extends CustomSinkBase {
    private static final Logger LOG = LoggerFactory.getLogger(PrintCustomSink.class);

    private LogProducer client;

    /*
     * LogProducer factory.
     */
    private LogProducerProvider logProducerProvider;

    private String project;

    private String logStore;

    private String endPoint;

    private String propertyFilePath;

    private String accessId;

    private String accessKey;

    private int maxRetryTime;

    private int flushInterval;

    private int updateSeconds;

    private int columnSize;

    private int mcIndex;

    private int vcIndex;

    private AtomicLong numFailed = new AtomicLong(0);

    @Override
    public void open(int i, int i1) throws IOException {
        project = userParamsMap.get("your_project_name");
        logStore = userParamsMap.get("your_logStore_value");
        endPoint = userParamsMap.get("your_endPoint_value");
        propertyFilePath = userParamsMap.get("your_propertyFilePath_value");
        accessId = userParamsMap.get("your_accessId_value");
        accessKey = userParamsMap.get("your_accessKey_value");
        String maxRetryTimeStr = userParamsMap.get("your_maxRetryTime_value");
        String flushIntervalStr = userParamsMap.get("your_flushInterval_value");
        String updateSecondsStr = userParamsMap.get("your_updateSeconds_value");

        String metricColumn = userParamsMap.get("your_metricColumn_value");
        String valueColumn = userParamsMap.get("your_valueColumn_value");

        if(StringUtils.isNullOrWhitespaceOnly(project)){
            throw new IllegalArgumentException("sls project cannot be null");
        }

        if(StringUtils.isNullOrWhitespaceOnly(logStore)){
            throw new IllegalArgumentException("sls logstore cannot be null");
        }

        if(StringUtils.isNullOrWhitespaceOnly(endPoint)){
            throw new IllegalArgumentException("sls endPoint cannot be null");
        }

        if (StringUtils.isNullOrWhitespaceOnly(metricColumn)){
            throw new IllegalArgumentException("metric column cannot be null");
        }

        if (StringUtils.isNullOrWhitespaceOnly(valueColumn)){
            throw new IllegalArgumentException("metric column cannot be null");
        }

        if(maxRetryTimeStr == null){
            maxRetryTime = 3;
        }

        if (flushIntervalStr == null){
            flushInterval = 2000;
        }

        if(updateSecondsStr == null){
            updateSeconds = 7200;
        }

        if (!StringUtils.isNullOrWhitespaceOnly(accessKey) && !StringUtils.isNullOrWhitespaceOnly(accessId)) {
            logProducerProvider = new LogProducerProvider(
                    project,
                    endPoint,
                    accessId,
                    accessKey,
                    maxRetryTime,
                    flushInterval,
                    updateSeconds);
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(propertyFilePath)) {
                logProducerProvider = new LogProducerProvider(
                        project,
                        endPoint,
                        propertyFilePath,
                        maxRetryTime,
                        flushInterval,
                        updateSeconds);
            } else {
                throw new IllegalArgumentException("AccessKey and accessId are not both configured, propertyFilePath also not configured.");
            }
        }

        // initial
        columnSize = rowTypeInfo.getTotalFields();
        mcIndex = rowTypeInfo.getFieldIndex(metricColumn);
        vcIndex = rowTypeInfo.getFieldIndex(valueColumn);

        client = logProducerProvider.getClient();

    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (InterruptedException e) {
                // ignore interrupt signal to avoid io thread leaking.
            } catch (ProducerException e) {
                LOG.warn("Exception caught when closing client.", e);
            }
            client = null;
        }
    }

    @Override
    public void writeAddRecord(Row row) throws IOException {
        /*
        add your logic here
         */
        String metricName = (String) row.getField(mcIndex);
        String metricValue = (String) row.getField(vcIndex);

        metricName = "data.xxx" + "." + metricName;

        MetricMessage metricMessage = new MetricMessage(metricName, 1L, Double.valueOf(metricValue), new HashMap<String, String>());
        LogItem logItem = new LogItem();
        logItem.PushBack(metricName, metricMessage.toString());
        try {
            logProducerProvider.getClient().send(project, logStore, logItem);
        } catch (InterruptedException | ProducerException e) {
            long sentFailedTimes = numFailed.incrementAndGet();
            if (sentFailedTimes % 200 == 1) {
                LOG.warn("Already fail to send metrics out " + sentFailedTimes + " times.", e);
            }
        }
    }

    @Override
    public void writeDeleteRecord(Row row) throws IOException {

    }

    @Override
    public void sync() throws IOException {
        System.out.println("Sync Called!");
    }

    @Override
    public String getName() {
        return "PrintCustomSink";
    }
}
