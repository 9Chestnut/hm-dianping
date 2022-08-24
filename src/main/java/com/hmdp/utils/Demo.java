package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;

import java.util.ArrayList;
import java.util.List;

public class Demo {
    public static void main(String[] args) {
        List<String> objects = get(2);
        System.out.println(StrUtil.join(",", objects));
    }

    public static List<String> get(int age){
        if (age ==2){
            return null;

        }
        return new ArrayList<String>();

    }
}
