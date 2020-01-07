<?php
require_once(__DIR__ . '/../vendor/autoload.php');

use \RabbitMqHelper\RabbitMQRetry\RabbitMqInstance;

$statTime = date('Y-m-d H:i:s');
define('RUNTIME', '[' . $statTime . '] ');
echo RUNTIME . " Starting! " . PHP_EOL;

// RabbitMq 连接配置
$rabbitMqConfig['demo'] = [
    'host' => '10.10.30.211',
    'port' => 5672,
    'user' => 'wms_server',
    'password' => 'jf67DfRghk32f8o0',
    'vhost' => 'wms',
];

try {
    $isDelayed = false; // 是否为延时队列
    $mqCls = RabbitMqInstance::getInstance($rabbitMqConfig['demo']);

    // 读消息
    $ret = $mqCls->getQueueMessage('demo.user.del', 'demo.user.delete', 'demoExChange', 10, $isDelayed, '');

    // 声明队列
    $mqCls->declareQueue('demo.user.add', 'demo.user.*', 'demoExChange', $isDelayed);
    $mqCls->declareQueue('demo.user.del', 'demo.user.delete', 'demoExChange', $isDelayed);

    //生产者 发送消息
    $publisher = $mqCls->getPublisherByExChange('demoExChange', $isDelayed);

    for($i = 0; $i <= 100; $i++) {
        $data = [
            'user_name' => 'demo_' . $i,
            'user_id' => $i,
            'user_password' => md5(rand(0, 1000) . time()),
            'add_date' => date('Y-m-d H:i:s'),
        ];
        $source = 'mq_source'; // 消息来源
        $message = new \RabbitMqHelper\RabbitMQRetry\Message($data, $source);
        try {
            $publisher->publish($message, 'demo.user.add', true, 30); // 延时 30 秒
            $publisher->publish($message, 'demo.user.delete', true, 10);
            $publisher->publish($message, 'demo.user.edit', true);
        } catch (\Exception $e) {
            // 发生异常....
        }
    }

    //启动消息消费者监听(阻塞模式)
    //routingKey 路由匹配模式 * 匹配一个单词，# 匹配0个或者多个单词
    //如果 “*” and “#” 没有被使用，那么topic exchange就变成了direct exchange
    $subConfig = [
        'queueName' => 'demo.user.add',
        'routingKey' => 'demo.user.add',
        'exchangeName' => 'demoExChange'
    ];
    $mqCls->execSubscriber($subConfig, function (\RabbitMqHelper\RabbitMQRetry\SubMessage $message) {
        // TODO 业务逻辑实现
        echo date('Y-m-d H:i:s') . PHP_EOL;
        echo sprintf(
            "subscriber Message:\n RoutingKey <%s>\n MessageBody: %s\n",
            $message->getRoutingKey(),
            $message->getMessage()->serialize()
        );
        echo "================================================\n";

        // 业务处理逻辑


        // ask 处理成功返回 success (返回 success 后将会对消息进行处理确认)，失败返回 failure （消息会重试处理）
        $returnSuccess = ['ask' => 'Success', 'message' => 'isSuccess'];
        $returnFailure = ['ask' => 'Failure', 'message' => 'Failure Message'];

        return $returnSuccess;
    }, false, $isDelayed);
} catch (Exception $e) {
    echo '[' . date('Y-m-d H:i:s') . '] Fail Exception:' . $e->getMessage() . PHP_EOL;
}

$endTime = date('Y-m-d H:i:s');
echo "runEndTime:" . (strtotime($endTime) - strtotime($statTime)) . PHP_EOL;
echo "[" . $endTime . "] End run\r\n";
