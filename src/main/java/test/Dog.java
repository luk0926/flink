package test;

/**
 * @BelongsProject: flink
 * @BelongsPackage: test
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-24 16:30
 */

public class Dog {
    String name;
    int age;

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public Dog() {
    }

    public Dog(String name, int age) {
        this.name = name;
        this.age = age;
    }
}