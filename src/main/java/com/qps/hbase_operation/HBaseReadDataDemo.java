package com.qps.hbase_operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table; 
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author tiotd
 *
 */
public class HBaseReadDataDemo {
	// 用于链接hbase的连接器对象，类似于mysql jdbc的Connection
	public Connection conn;
	// 用hbase Configuration初始化配置信息时会自动加载当前应用classpath下的hbase-site.xml
	public static Configuration conf = HBaseConfiguration.create();

	// 初始化hbase操作对象
	public HBaseReadDataDemo() throws Exception {
		// ad = new HBaseAdmin(conf);过期，推荐使用Admin
		conf.set("hbase.zookeeper.quorum",
				"192.168.1.34,192.168.1.31,192.168.1.32,192.168.1.41");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-ensecure");
		// Connection初始化
		conn = ConnectionFactory.createConnection(conf);
	}

	// 创建表
	public void createTable(String tablename, String... cf1) throws Exception {
		// 获取admin对象
		Admin admin = conn.getAdmin();
		// 创建tablename对象描述表的名称信息
		TableName tname = TableName.valueOf(tablename);
		// 创建HTableDescriptor对象，描述表信息
		HTableDescriptor tDescriptor = new HTableDescriptor(tname);
		// 判断表是否存在
		if (admin.tableExists(tname)) {
			System.out.println("表" + tablename + "已存在！");
			return;
		}

		// 添加表列簇信息
		for (String cf : cf1) {
			HColumnDescriptor cFamily = new HColumnDescriptor(cf);
			tDescriptor.addFamily(cFamily);
		}
		// 调用admin的createtable方法创建表
		admin.createTable(tDescriptor);
		System.out.println("表" + tablename + "创建成功！");
	}

	// 删除表
	public void deleteTable(String tablename)throws Exception{
		Admin admin=conn.getAdmin();
		TableName tName=TableName.valueOf(tablename);
		if(admin.tableExists(tName)){
			admin.disableTable(tName);
			admin.deleteTable(tName);
			System.out.println("删除表"+tablename+"成功！");
		}else{
			System.out.println("表"+tablename+"不存在。");
		}
	}
	
	//新增数据到表里面Put 
	public void putData(String table_name)throws Exception{
		TableName tableName=TableName.valueOf(table_name);
		Table table=conn.getTable(tableName);
		Random random = new Random();
		List<Put> batPut = new ArrayList<Put>();
		for(int i=0; i<10; i++){
			//构建put的参数是rowkey rowkey_i(Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
			Put put = new Put(Bytes.toBytes("rowkey_"+ i));
			put.addColumn(Bytes.toBytes("user"),Bytes.toBytes("username"),Bytes.toBytes("un_"+i));
			put.addColumn(Bytes.toBytes("user"),Bytes.toBytes("age"),Bytes.toBytes(random.nextInt(50)+1));
			put.addColumn(Bytes.toBytes("user"),Bytes.toBytes("birthday"),Bytes.toBytes("20170"+i+"01"));
			put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("phone"),Bytes.toBytes("电话_"+i));
			put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("email"),Bytes.toBytes("email_"+i));
			//单条记录put
			//table.put(put);
			batPut.add(put);
		}
		table.put(batPut);
		System.out.println("插入成功！");
	}
	//查询数据
	public void getData(String table_name) throws Exception {
		TableName tableName = TableName.valueOf(table_name);
		Table table = conn.getTable(tableName);
		//构建get对象
		List<Get> gets = new ArrayList<Get>();
		for (int i = 0; i < 5; i++) {
			Get get = new Get(Bytes.toBytes("rowkey_"+ i));
			gets.add(get);
		}
		Result[] results = table.get(gets);
		for (Result result: results) {
			//使用cell获取result里面的数据
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				//从单元格cell内把数据获取并输出
				//使用CellUtil工具类，把cell中数据获取出来
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println("family:" + family + "\nqualify:" + qualify + "\nrowkey:" + rowkey + "\nvalue" + value);
			}
		}
	}
	
	//关闭连接
	public void CleanUp() throws Exception{
		conn.close();
	}
	

	public static void main(String[] args) throws Exception {
		HBaseReadDataDemo hbaseReadDataDemo = new HBaseReadDataDemo();
		String tablename = "test";
		hbaseReadDataDemo.createTable(tablename, "haha", "shenme");
		hbaseReadDataDemo.putData(tablename);
		hbaseReadDataDemo.getData(tablename);
		hbaseReadDataDemo.deleteTable(tablename);
		hbaseReadDataDemo.CleanUp();
	}

}
