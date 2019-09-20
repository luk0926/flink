import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @BelongsProject: flink
 * @BelongsPackage: PACKAGE_NAME
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-09-18 11:04
 */

public class LineCount {
    public static void main(String[] args) {
        //输入路径
        String dir = "D:\\github\\flink\\src";
        File file = new File(dir);

        HashMap<String, Integer> hashMap = new HashMap<>();//用来存放统计出来的行数


        File_list(file, hashMap);
        getResult(hashMap);
    }

    //遍历文件夹
    public static void File_list(File file, HashMap hashMap) {
        File[] files = file.listFiles();//获取传入路径的所有文件

        //遍历文件
        for (File f : files) {
            if (f.isDirectory()) {
                File_list(f, hashMap);
            } else {
                //否者调方法统计行数
                lineNumber(f.getAbsolutePath(), hashMap);
            }
        }
    }

    //统计相应文件的行数
    public static HashMap<String, Integer> lineNumber(String f, HashMap hashMap) {
        //定义字符流读取文件
        FileReader fileReader = null;
        try {
            fileReader = new FileReader(f);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("输入的路径不正确");
        }

        //从字节流中升级为字符流，方便按行读取。
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        int index = 0;

        try {
            while (bufferedReader.readLine() != null) {
                index++;
            }
            hashMap.put(f, index);

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("这个文件读不到");
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return hashMap;
        }
    }

    //把文件的行数存放在一个map中，然后输出行数之和
    public static void getResult(HashMap hashMap){
        int sum = 0;
        //使用iterator遍历map集合
        Iterator<Map.Entry<String,Integer>> entries = hashMap.entrySet().iterator();
        while (entries.hasNext()){
            Map.Entry<String, Integer> next = entries.next();
            System.out.println(next.getKey()+"的行数是:"+next.getValue());
            sum+=next.getValue();
        }

        System.out.println("总行数：" + sum);
    }

}