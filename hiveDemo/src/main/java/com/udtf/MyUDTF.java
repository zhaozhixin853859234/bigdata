package com.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-10 19:54
 */
public class MyUDTF extends GenericUDTF {
    private List<String> dataList = new ArrayList<String>();
    // 定义输出数据的列名和数据类型（StructObjectInspector结构体类型检验）
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 定义输出数据的列名,列名的类型为String
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word");

        // 定义输出数据的数据类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * Give a set of arguments for the UDTF to process.
     * 初始化完成之后会调用process方法,真正的处理过程在process函数中，
     * 在process中，每一次forward()调用产生一行；
     * 如果产生多列可以将多个列的值放在一个数组中，然后将该数组传入到forward()函数。
     * @param args object array of arguments
     */

    @Override
    public void process(Object[] args) throws HiveException {
        // 输入的字符串
        String inputStr = args[0].toString();
        // 字符串分隔符
        String splitKey =  args[1].toString();

        // 将字符串分割为字符串数组
        String[] words = inputStr.split(splitKey);

        // 输出数据
        for (String word : words) {
            // 将数据放置集合中输出
            dataList.clear();
            dataList.add(word);
            forward(dataList);
        }
    }

    /**
     * Called to notify the UDTF that there are no more rows to process.
     * Clean up code or additional forward() calls can be made here.
     */
    @Override
    public void close() throws HiveException {

    }
}
