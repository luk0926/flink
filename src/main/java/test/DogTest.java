package test;

/**
 * @BelongsProject: flink
 * @BelongsPackage: test
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-24 16:50
 */

public class DogTest {
    public static void main(String[] args) {
        Dog a = new Dog("a", 100);
        String name = a.getName();
        int age = a.getAge();

        System.out.println(name + "\t" + age);
    }
}