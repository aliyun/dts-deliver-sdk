package com.aliyun.dts.deliver.framework.processor;

import com.aliyun.dts.deliver.commons.concurrency.Promise;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class FutureRecords<T> extends Promise<T> {

    private InflightRecordHead head;

    private int size;

    public FutureRecords(List<Record> records) {
        this.head = new InflightRecordHead();

        this.size = records.size();

        records.forEach(record -> {
            InflightRecord inFlightRequest = new InflightRecord(record);
            add(inFlightRequest);
        });
    }

    /**
     * Add the given request to the queue
     */
    public void add(InflightRecord request) {
        Preconditions.checkState(head != null);
        InflightRecord tail = head.getPrev();
        Preconditions.checkState(tail != null);

        // add current checkpoint to the tail
        tail.setNext(request);
        request.setPrev(tail);
        request.setNext(head);
        head.setPrev(request);
    }

    public void complete(InflightRecord inFlightRequest) {
        inFlightRequest.setConsumed(true);

        InflightRecord realRequest = head;
        InflightRecord next = realRequest.getNext();

        //get the next consumed InFlightRequest
        while (next != head && next.isConsumed()) {
            realRequest = next;
            next = realRequest.getNext();
        }

        head.setNext(realRequest.getNext());
        realRequest.getNext().setPrev(head);

        if (isEmpty()) {
            success(null);
        }
    }

    public boolean isEmpty() {
        return head.next.equals(head) ? true : false;
    }

    public List<InflightRecord> getInFlightRequests() {
        List<InflightRecord> inFlightRequests = new ArrayList<>();
        InflightRecord next = head.getNext();
        while (next != head) {
            inFlightRequests.add(next);
            next = next.getNext();
        }
        return inFlightRequests;
    }

    public InflightRecord peekFirst() {
        return head.next;
    }

    public int size() {
        return size;
    }

    static class InflightRecordHead extends InflightRecord {

        InflightRecordHead() {
            super(null);
            setNext(this);
            setPrev(this);
        }
    }
}
