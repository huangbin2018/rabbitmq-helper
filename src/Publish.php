<?php

namespace RabbitMqHelper\RabbitMQRetry;

use PhpAmqpLib\Message\AMQPMessage;

interface Publish
{
    /**
     * Publish a message to mq
     *
     * @param \Serializable|array $message
     * @param string              $routingKey
     *
     * @return AMQPMessage
     */
    public function publish($message, string $routingKey);
}
