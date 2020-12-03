package com.stringBuilder;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-04 09:59
 */
public class StringBuilderDiffTest {
    public static void main(String[] args) {
        StringBuilder sb1 = new StringBuilder("abc");
        StringBuilder sb2 = new StringBuilder("abc");
        System.out.println(sb1.equals(sb2));
        System.out.println(sb1==sb2);
    }
}
