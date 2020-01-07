<?php

namespace RabbitMqHelper\RabbitMQRetry;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exchange\AMQPExchangeType;

/**
 * Class PubSub
 *
 * @package RabbitMqHelper\RabbitMQRetry
 */
abstract class PubSub
{

    /**
     * @var RabbitMQ
     */
    protected $rabbitMQ;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var string
     */
    protected $exchangeName;

    /**
     * Publisher constructor.
     *
     * @param RabbitMQ|null $rabbitMQ
     * @param string        $exchangeName
     * @param bool          $isDelayed
     */
    public function __construct(RabbitMQ $rabbitMQ = null, string $exchangeName = 'master', $isDelayed = false)
    {
        $this->rabbitMQ     = $rabbitMQ;
        $this->exchangeName = $exchangeName;
        $this->initialize($isDelayed);
    }

    /**
     * Generate Routing Key
     *
     * @param string $entity
     * @param string $action
     *
     * @return string
     */
    public function routingKey(string $entity, string $action): string
    {
        return sprintf('%s.%s', $entity, $action);
    }

    /**
     * Parse the routing key
     *
     * @param string $routingKey
     *
     * @return array [entity, action]
     */
    public function parseRoutingKey(string $routingKey): array
    {
        $segs   = explode('.', $routingKey);
        $action = $segs[count($segs) - 1];
        // 注意删除顺序，先后面的元素，后前面的元素
        // 如果反过来会导致删除后面元素的时候索引值变了，导致删除错误的索引
        unset($segs[count($segs) - 1], $segs[0]);

        return [
            implode('.', $segs),
            $action
        ];
    }

    /**
     * Initialize
     * @param bool $isDelayed
     * @return void
     */
    protected function initialize($isDelayed = false)
    {
        $this->channel = $this->rabbitMQ->channel();
        if ($isDelayed) {
            // 延时消息交换机
            $ticket = new \PhpAmqpLib\Wire\AMQPTable(array(
                "x-delayed-type" => AMQPExchangeType::TOPIC,
            ));
            $this->channel->exchange_declare($this->exchangeTopic(), 'x-delayed-message', false, true, false, false, false, $ticket);
        } else {
            // 普通交换机
            $this->channel->exchange_declare($this->exchangeTopic(), AMQPExchangeType::TOPIC, false, true, false);
        }

        // 重试交换机
        $this->channel->exchange_declare($this->exchangeRetryTopic(), AMQPExchangeType::TOPIC, false, true, false);
        // 失败交换机
        $this->channel->exchange_declare($this->exchangeFailedTopic(), AMQPExchangeType::TOPIC, false, true, false);
    }

    /**
     * 获取交换机Topic
     *
     * @return string
     */
    protected function exchangeTopic(): string
    {
        return $this->exchangeName;
    }

    /**
     * 重试交换机Topic
     *
     * @return string
     */
    protected function exchangeRetryTopic(): string
    {
        return $this->exchangeTopic() . '.retry';
    }

    /**
     * 失败交换机Topic
     *
     * @return string
     */
    protected function exchangeFailedTopic(): string
    {
        return $this->exchangeTopic() . '.failed';
    }

    /**
     * 关闭 MQ 连接
     */
    public function closeMQ()
    {
        $channel = $this->rabbitMQ->channel();
        $connection = $this->rabbitMQ->connection();
        $channel->close();
        $connection->close();
    }
}
