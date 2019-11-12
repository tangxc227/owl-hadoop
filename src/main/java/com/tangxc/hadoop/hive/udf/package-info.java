/**
 * @author Xicheng.Tang
 *
 * 自定义UDF
 *
 * 一、自定义UDF(一个输入，一个输出)
 *  1. 添加jar：add jar xxx.jar;
 *  2. 创建function:
 *      临时function： create temporary function temp_f as 'com.tangxc.hadoop.hive.udf.UdfLowerOrUpperCase';
 *      永久function: create function lower_upper as 'com.tangxc.hadoop.hive.udf.UdfLowerOrUpperCase';
 *  3. 使用
 *      select lower_upper('HellO')
 *
 * 二、自定义UDAF(多个输入，一个输出) 和group by一起操作
 *  1. UDAF介绍
 *      PARTIAL1：iterate&terminatePartial  map的输入阶段
 *      PARTIAL2: merge&terminatePartial map输出阶段
 *      FINAL: merge&terminate reducer输入&reducer输出阶段
 *      COMPLETE: iterate&terminate 没有reducer的map输出阶段
 *  2. UDAF实例：实现自定义sum函数
 *      create function self_sum as 'com.tangxc.hadoop.hive.udf.UdafSumCase';
 *
 * 三、自定义UDTF(一个输入，多个输出)
 *  1. 创建函数
 *      create function f1 as 'com.tangxc.hadoop.hive.udf.UdtfCase';
 *
 */
package com.tangxc.hadoop.hive.udf;