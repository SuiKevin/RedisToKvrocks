# RedisToKvrocks
Redis数据迁移至Kvrocks

Redis版本>7.0时，RedisShake等工具无法使用，特此建立此脚本
任意版本均可迁移

# 运行
ThinkPHP环境中
使用命令行运行，防止超时
```
php /www/wwwroot/127.0.0.1/index.php api/sync/sync
```
