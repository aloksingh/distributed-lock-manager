package com.alok.lock.util;

import org.apache.log4j.lf5.StartLogFactor5;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Util {
    private static final String ISO_UTC_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public String toISOUtcString(long time) {
        Date date = new Date(time);
        return toISOUtcString(date);
    }

    public String toISOUtcString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat(ISO_UTC_PATTERN);
        return format.format(date);
    }

    public Date fromISOUtcString(String utcDate) {
        SimpleDateFormat format = new SimpleDateFormat(ISO_UTC_PATTERN);
        try {
            return format.parse(utcDate);
        } catch (Exception e) {
        }
        return null;
    }

    public List<Integer> range(int start, int end) {
        List<Integer> range = new ArrayList<Integer>();
        for(int i = start; i< end; i++){
            range.add(i);
        }
        return range;
    }
}
