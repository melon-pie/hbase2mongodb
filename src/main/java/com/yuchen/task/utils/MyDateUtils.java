package com.yuchen.task.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/** 未使用，仅供参考
 * */
public class MyDateUtils {
    public  static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
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


    //日期减七天->Date
    public  static String subDateSeven(Date startDate){
        Calendar date =Calendar.getInstance();
        date.setTime(startDate);
        date.add(date.DATE ,-7);

        return sdf.format(date.getTime());
    }

    //日期减3天->Date
    public  static String subDateThree(Date startDate){
        Calendar date =Calendar.getInstance();
        date.setTime(startDate);
        date.add(date.DATE ,-3);

        return sdf.format(date.getTime());
    }



    // 获取before天前时间戳
    public static long getBeforTimestamp(int before){
        Calendar date =Calendar.getInstance();
        date.setTime(new Date());
        date.add(date.DATE ,before);
        return date.getTimeInMillis();
    }


    public static long getZeroTimestamp(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
    // 2小时前时间戳
    public  static long subDateTwoHour(){
        Calendar date =Calendar.getInstance();
        date.setTime(new Date());
        date.add(Calendar.HOUR ,-2);
        date.add(Calendar.MINUTE ,-15);
        return date.getTimeInMillis();
    }
    // 一小时前时间戳
    public  static long subDateOneHour(){
        Calendar date =Calendar.getInstance();
        date.setTime(new Date());
        date.add(Calendar.HOUR ,-1);
        date.add(Calendar.MINUTE ,-15);
        return date.getTimeInMillis();
    }

    // 十五分钟前时间戳
    public  static long sub15Minute(){
        Calendar date =Calendar.getInstance();
        date.setTime(new Date());
        date.add(Calendar.MINUTE ,-15);
        return date.getTimeInMillis();
    }
    // 获取当前时间戳
    public static long getCurrentTimestamp(){
        Calendar calendar = Calendar.getInstance();
        return calendar.getTimeInMillis();
    }
    //
    public  static String nowDate(Date startDate){
        return sdf.format(startDate);
    }

}
