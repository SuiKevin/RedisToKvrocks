<?php
// +----------------------------------------------------------------------
// | Redis迁移脚本 by Kevin 20230728
// +----------------------------------------------------------------------
// | 当Redis版本大于7.0时，迁移工具如RedisShake等无法使用，特此建立此脚本
// | 此脚本是在thinkphp中运行的，通过命令行调用，防止超时
// +----------------------------------------------------------------------
// | Author: Kevin <kevinsui666@163.com> <http://www.kevinsui.cn>
// +----------------------------------------------------------------------

namespace app\api\controller;

use think\Db;
use app\common\controller\RedisConnect;

class Sync
{
    //redis key类型常量，redis type命令返回类型为整数
    private const REDIS_STRING = 1;
    private const REDIS_SET = 2;
    private const REDIS_LIST = 3;
    private const REDIS_ZSET = 4;
    private const REDIS_HASH = 5;
    private const REDIS_STREAM = 6;

    public function index(){
        echo 'redis export';
    }

    /**
     * redis迁移至kvrocks
     *
     */
    public function sync(){
        //连接kvrocks
        $kvrocks = new \Redis();
        // Connect to the Kvrocks server
        $kvrocks->connect('127.0.0.1', 6379);

        //连接redis
        $redis = new \Redis();
        // Connect to the Redis server
        $redis->connect('127.0.0.1', 6378);

        //获取redis中的所有key
        $keys = $redis->keys('*');
        echo 'Redis dbsize'.count($keys)."\n";

        //过滤key
        $filter = ["redis-crm-data-list"];

        //输出序号
        $index = 0;

        //循环redis的key，导入到kvrocks
        foreach ($keys as $key) {
            //序号
            $index+=1;
            echo $index.'-'.$key."\n";

            //过滤key
            if(in_array($key, $filter)){
                continue;
            }
            //导入kvrocks
            $this->export_redis($key, $redis,$kvrocks);
        }
        echo 'done';
    }

    /**
     * Export to kvrocks
     * 分类导入kvrocks
     * @param $key
     * @param $redis
     * @param $kvrocks
     */
    function export_redis($key, $redis, $kvrocks) {
        //导入间隔毫秒，防止过快导入redis崩溃
        $sleeptime = 300;

        //异常等待时间秒
        $exceptiontime = 90;
        /**
         *  #!/bin/bash
            #监控redis进程
            count=0
            count=$(ps aux|grep redis-server|wc -l)
            #  echo `ps aux|grep redis-server`

            time=$(date "+%Y-%m-%d %H:%M:%S")

            echo "[$time] redis-pscount:$count"

            if [ $count -lt 2 ]
            then
                echo "[$time] redis [CRASHED]"
                echo "[$time] redis [STARTING]"
                /usr/sbin/service redis start
                echo "[$time] redis [STARTED]"
            else
                echo "[$time] redis is running on PID `cat /www/server/redis/redis.pid`"
            fi
         */

        //获取key类型
        $type = $redis->type($key);

        //分类导入
        switch ($type) {
            case 1:
                //$type = 'string';
                //导入kvrocks
                $kvrocks->set($key, $redis->get($key));

                //运行间隔
                usleep($sleeptime);

                //设置过期时间
                $ttl = $redis->ttl($key);
                if($ttl>0){
                    $kvrocks->expire($key,$ttl);
                }
                break;
            case 2:
                //$type = 'set';
                $values = $redis->sMembers($key);
                foreach ($values as $v) {
                    try{
                        $redis->sAdd($key, $v);
                        usleep($sleeptime);
                    }catch(\Exception $e){
                        sleep($exceptiontime);
                        echo $e->getMessage()."\n";
                        echo "sleep ".$exceptiontime."s\n";
                        $redis->sAdd($key, $v);
                    }
                    echo $key.'-'.$v."\n";
                }
                $ttl = $redis->ttl($key);
                if($ttl>0){
                    $kvrocks->expire($key,$ttl);
                }
                break;
            case 3:
                //$type = 'list';
                $size = $redis->lLen($key);
                for ($i = 0; $i < $size; ++$i) {
                    try{
                        $redis->rPush($key, $redis->lIndex($key, $i));
                        usleep($sleeptime);
                    }catch(\Exception $e){
                        sleep($exceptiontime);
                        echo $e->getMessage()."\n";
                        echo "sleep ".$exceptiontime."s\n";
                        $redis->rPush($key, $redis->lIndex($key, $i));
                    }
                    echo $key.'-'.$i."\n";
                }
                $ttl = $redis->ttl($key);
                if($ttl>0){
                    $kvrocks->expire($key,$ttl);
                }
                break;
            case 4:
                //$type = 'zset';
                $values = $redis->zRange($key, 0, -1);

                foreach ($values as $v) {
                    $s = $redis->zScore($key, $v);
                    $redis->zAdd($key, $s, $v);
                    try{
                        $redis->zAdd($key, $s, $v);
                        usleep($sleeptime);
                    }catch(\Exception $e){
                        sleep($exceptiontime);
                        echo $e->getMessage()."\n";
                        echo "sleep ".$exceptiontime."s\n";
                        $redis->zAdd($key, $s, $v);
                    }
                    echo $key.'-'.$v."\n";
                }
                $ttl = $redis->ttl($key);
                if($ttl>0){
                    $kvrocks->expire($key,$ttl);
                }
                break;
            case 5:
                //$type = 'hash';
                $values = $redis->hGetAll($key);
                foreach ($values as $k => $v) {
                    try{
                        $kvrocks->hSet($key, $k, $v);
                        usleep($sleeptime);
                    }catch(\Exception $e){
                        sleep($exceptiontime);
                        echo $e->getMessage()."\n";
                        echo "sleep ".$exceptiontime."s\n";
                        $kvrocks->hSet($key, $k, $v);
                    }
                    echo $k."\n";
                }
                $ttl = $redis->ttl($key);
                if($ttl>0){
                    $kvrocks->expire($key,$ttl);
                }
                break;

            default:
                // code...
                break;
        }
    }

}
