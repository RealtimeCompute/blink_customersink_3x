package com.alibaba.blink.customersink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.PriorityType;
import org.apache.flink.types.Row;

import java.io.IOException;

public class PrintCustomSink1 extends CustomSinkBase {

    private String tpsMetricName;
    private String bpsMetricName;

    private KMonitor kMonitor;

    public void open(int i, int i1) throws IOException {

        /**
         * 如何汇报tps和bps指标
         */
        String projectName = userParamsMap.get("__inner__projectname__");
        String jobName = userParamsMap.get("__inner__jobname__");
        System.out.println(projectName);
        System.out.println(jobName);
        kMonitor = KMonitorFactory.getKMonitor("");
        if (!KMonitorFactory.isStarted()) {
            KMonitorFactory.start();
        }
        System.out.println(projectName + "." + jobName + "." + "sink.outTps");
        tpsMetricName = projectName + "." + jobName + "." + "sink.outTps.rate";

        kMonitor.register(tpsMetricName, MetricType.QPS, PriorityType.NORMAL);
    }

    public void close() throws IOException {

    }

    public void writeAddRecord(Row row) throws IOException {
        System.out.println(row);
        kMonitor.report(tpsMetricName, 1);
    }

    public void writeDeleteRecord(Row row) throws IOException {

    }

    public void sync() throws IOException {
        System.out.println("Sync Called!");
    }

    public String getName() {
        return "PrintCustomSink";
    }
}
