<?php

namespace RabbitMqHelper\RabbitMQRetry;

use Publisher;
use SubscribeAbstract;
use RabbitMQ;
use Message;

//是否记录消费日志 open-记录，close-不记录
defined('MQ_SUBSCRIBER_EXEC_LOG') || define('MQ_SUBSCRIBER_EXEC_LOG', 'close');

//异常重试次数
defined('RABBITMQ_RETRYCOUNT') || define('RABBITMQ_RETRYCOUNT', 5);

//死信消息TTL (毫秒)
defined('RABBITMQ_DEADLETTERTTL') || define('RABBITMQ_DEADLETTERTTL', 1000 * 60);

// 连接配置定义
defined('RABBITMQ_HOST') || define('RABBITMQ_HOST', '127.0.0.1');
defined('RABBITMQ_PORT') || define('RABBITMQ_PORT', 5672);
defined('RABBITMQ_USER') || define('RABBITMQ_USER', 'user');
defined('RABBITMQ_PASS') || define('RABBITMQ_PASS', 'password');
defined('RABBITMQ_VHOST') || define('RABBITMQ_VHOST', '/');


/**
 * @Class   RabbitMqInstance
 * @Desc
 * @author  Huangbin <huangbin2018@qq.com>
 * @Date    2019/4/10 17:09
 * @package RabbitMqHelper\RabbitMQRetry
 */
class RabbitMqInstance
{

    /**
     * @var \System\JWT|null 静态成品变量保存全局实例
     */
    private static $_instance = null;

    /**
     * @var RabbitMQ|null
     */
    protected $rabbitMQ = null;

    public function __construct($configs = array())
    {
        if (empty($configs)) {
            $configs = [
                'host' => RABBITMQ_HOST,
                'port' => RABBITMQ_PORT,
                'user' => RABBITMQ_USER,
                'password' => RABBITMQ_PASS,
                'vhost' => RABBITMQ_VHOST,
            ];
        }

        $this->rabbitMQ = new RabbitMQ($configs);
    }

    /**
     * 静态工厂方法，返还此类的唯一实例
     *
     * @param array $configs
     *
     * @return RabbitMqInstance|null
     */
    public static function getInstance($configs = array())
    {
        if (is_null(self::$_instance)) {
            self::$_instance = new self($configs);
        }

        return self::$_instance;
    }

    /**
     * 获取 RabbitMQ 连接
     *
     * @return RabbitMQ|null
     */
    public function getRabbitMQ()
    {
        return $this->rabbitMQ;
    }

    //===================================================================================
    // 消息生产者 Start...
    // <code>
    // # 发送消息 demo...
    // $message = new \RabbitMqHelper\RabbitMQRetry\Message($data);
    // $routingKey = 'routing.demo';
    // ClsRabbitMq::getInstance()->getPublisherByExChange('demoExChange')->publish($message, $routingKey);
    // </code>
    //===================================================================================
    /**
     * 获取一个指定 $exChangeName 的消息生产者
     * @author Huangbin <huangbin2018@qq.com>
     *
     * @param string $exChangeName 消息交换机名称
     * @param bool   $isDelayed 消息延时
     *
     * @return Publisher
     */
    public function getPublisherByExChange($exChangeName = '', $isDelayed = false)
    {
        $publisher = new Publisher($this->rabbitMQ, $exChangeName, $isDelayed);
        return $publisher;
    }

    /**
     * 获取指定 $data 序列化的消息
     *
     * @param array $data
     *
     * @return Message
     */
    public function getDataMessage($data = array())
    {
        $message = new Message($data);
        return $message;
    }
    //===================================================================================
    // 消息生产者 End...
    //===================================================================================

    /**
     * 执行消息消费(阻塞模式...)
     *
     * @param array $configs 消息配置 ['queueName' => '队列名称', 'routingKey' => '路由KEY', 'exchangeName' => '交换机名称']
     * @param null  $func 业务处理回调
     * @param bool  $retryFailedMessage 是否重放错误消息
     * @param bool  $isDelayed 消息延时
     */
    public function execSubscriber($configs = array(), $func = null, $retryFailedMessage = false, $isDelayed = false)
    {
        $subscriber = new SubscribeAbstract($this->rabbitMQ, $configs);
        $subscriber->isDelayed = $isDelayed;
        //重置失败消息
        if ($retryFailedMessage) {
            $subscriber->retryFailedMessage();
        }

        //监听消息队列(阻塞模式...)
        $subscriber->run($func);
    }

    /**
     * 声明创建队列绑定路由
     * @param string $queueName
     * @param string $routingKey
     * @param string $exchangeName
     * @param bool   $isDelayed 消息延时
     */
    public function declareQueue($queueName = '', $routingKey = '', $exchangeName = '', $isDelayed = false)
    {
        $configs = [
            'queueName' => $queueName,
            'routingKey' => $routingKey,
            'exchangeName' => $exchangeName
        ];
        $subscriber = new SubscribeAbstract($this->rabbitMQ, $configs);
        $subscriber->declareQueue($queueName, $routingKey, $exchangeName, $isDelayed);
    }

    public function getQueueMessage($queueName = '', $routingKey = '', $exchangeName = '', $limit = 10, $isDelayed = false, $type = 'failed')
    {
        $configs = [
            'queueName' => $queueName,
            'routingKey' => $routingKey,
            'exchangeName' => $exchangeName,
            'isDelayed' => $isDelayed,
        ];
        $subscriber = new SubscribeAbstract($this->rabbitMQ, $configs);
        return $subscriber->getQueueMessage($queueName, $routingKey, $exchangeName, $limit, $type);
    }
}
