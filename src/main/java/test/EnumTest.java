package test;

import java.util.ArrayList;

/**
 * @BelongsProject: flink
 * @BelongsPackage: test
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-24 14:46
 */

public class EnumTest {
    ArrayList<String> a = new ArrayList<String>();

    public String getArrayList() {

        this.a = new ArrayList<>();

        return "";
    }


    public static void main(String[] args) {
        FreshJuice large = FreshJuice.LARGE;

        System.out.println(large);
    }
}