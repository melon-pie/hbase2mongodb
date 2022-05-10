package com.yuchen.news.app.utils;

import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Component
public class MyDateUtils {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String DateToStr(Date date){
        return sdf.format(date);
    }

    public String DateToStr1(Date date){
        return sdf1.format(date);
    }

    public String DateToStr2(Date date){
        return sdf2.format(date);
    }

    public Date strToDate(String str){
        try {
            return sdf.parse(str);
        } catch (ParseException e) {
            return new Date();
        }
    }

    public Date strToDate1(String str){
        try {
            return sdf1.parse(str);
        } catch (ParseException e) {
            return new Date();
        }
    }

    //yyyy-MM-dd --> yyyy-MM-dd
    public String subDate(String startDate){
        Calendar date = Calendar.getInstance();
        date.setTime(strToDate1(startDate));
        date.set(Calendar.DATE, date.get(Calendar.DATE) - 1);
        return sdf1.format(date.getTime());
    }

    //yyyy-MM-dd --> yyyyMMdd
    public String subDate1(String startDate){
        Calendar date = Calendar.getInstance();
        date.setTime(strToDate1(startDate));
        date.set(Calendar.DATE, date.get(Calendar.DATE) - 1);
        return sdf.format(date.getTime());
    }

    //日期加一>字符串
    public  String addDateOneToStr(Date startDate){
        Calendar date =Calendar.getInstance();
        date.setTime(startDate);
        date.add(date.DATE ,1);
        return sdf.format(date.getTime());
    }

    //日期加一>Date
    public  Date addDateOne(Date startDate){
        Calendar date =Calendar.getInstance();
        date.setTime(startDate);
        date.add(date.DATE ,1);
        return date.getTime();
    }
    //日期减一->Date
    public  Date subDateOne(Date startDate){
        Calendar date =Calendar.getInstance();
        date.setTime(startDate);
        date.add(date.DATE ,-1);
        return date.getTime();
    }

}
