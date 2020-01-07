<?php

namespace RabbitMqHelper\RabbitMQRetry;

use PhpAmqpLib\Exception\AMQPIOWaitException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * Class Subscriber
 *
 * @package RabbitMqHelper\RabbitMQRetry
 */
class Subscriber extends PubSub implements Subscribe
{
    use Helpers;

    /**
     * 订阅消费
     *
     * 注意： 该方法会堵塞执行
     *
     * @param string   $queueName          队列名称
     * @param string   $routingKey         过滤路由key
     * @param \Closure $callback           回调处理函数，包含参数(AMQPMessage $msg, $publishRetry, $publishFailed)
     * @param \Closure $shouldExitCallback 是否应该退出的回调函数，返回true则退出，false继续执行
     * @param bool     $isDelayed
     * @throws \ErrorException
     */
    public function consume(
        string $queueName,
        string $routingKey,
        \Closure $callback,
        \Closure $shouldExitCallback = null,
        $isDelayed = false
    ) {
        if ($shouldExitCallback === null) {
            $shouldExitCallback = function () {
                return false;
            };
        }

        $this->declareRetryQueue($queueName, $routingKey);
        $this->declareConsumeQueue($queueName, $routingKey, $isDelayed);
        $this->declareFailedQueue($queueName, $routingKey);

        // 发起延时重试
        $publishRetry = function (AMQPMessage $msg, string $errMsg) use ($queueName) {

            /** @var AMQPTable $headers */
            if ($msg->has('application_headers')) {
                $headers = $msg->get('application_headers');
            } else {
                $headers = new AMQPTable();
            }

            $headers->set('x-orig-routing-key', $this->getOrigRoutingKey($msg));

            $properties = $msg->get_properties();
            $properties['application_headers'] = $headers;

            $msgOrg = $msg->getBody();
            if (!empty($errMsg)) {
                $msgOrgArr = json_decode($msgOrg, true);
                $msgOrgArr['__errMsg'] = $errMsg;
                $msgOrg = json_encode($msgOrgArr, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            }

            $newMsg = new AMQPMessage($msgOrg, $properties);

            $this->channel->basic_publish(
                $newMsg,
                $this->exchangeRetryTopic(),
                $queueName
            );
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        };

        // 将消息发送到失败队列
        $publishFailed = function (AMQPMessage $msg, string $errMsg) use ($queueName) {
            /** @var AMQPTable $headers */
            if ($msg->has('application_headers')) {
                $headers = $msg->get('application_headers');
            } else {
                $headers = new AMQPTable();
            }
            $headers->set('x-orig-routing-key', $this->getOrigRoutingKey($msg));
            $properties = $msg->get_properties();
            $properties['application_headers'] = $headers;
            $msgOrg = $msg->getBody();
            if (!empty($errMsg)) {
                $msgOrgArr = json_decode($msgOrg, true);
                $msgOrgArr['__errMsg'] = $errMsg;
                $msgOrg = json_encode($msgOrgArr, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            }
            $newMsg = new AMQPMessage($msgOrg, $properties);
            $this->channel->basic_publish(
                $newMsg,
                $this->exchangeFailedTopic(),
                $queueName
            );
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        };
        /*
        //$this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume(
            $queueName,
            '',
            false,
            false,
            false,
            false,
            function (AMQPMessage $msg) use ($callback, $publishRetry, $publishFailed) {
                $callback($msg, $publishRetry, $publishFailed);
            }
        );
        while (count($this->channel->callbacks)) {
            if ($shouldExitCallback()) {
                return;
            }

            try {
                $this->channel->wait(null, false, 3);
            } catch (AMQPTimeoutException $e) {
            } catch (AMQPIOWaitException $e) {
            }
        }
        */
        try {
            $connectionRequired = true;
            $connectionAttempts = 0; // 连接尝试计数器
            while ($connectionRequired && $connectionAttempts < 10) {
                $connectionAttempts++;
                $foundQueue = false;
                try {
                    // 尝试连接
                    $this->rabbitMQ->doConnect();
                    try {
                        $this->channel = $this->rabbitMQ->channel();
                        //$this->channel->basic_qos(null, 1, null);
                        $this->channel->basic_consume(
                            $queueName,
                            '',
                            false,
                            false,
                            false,
                            false,
                            function (AMQPMessage $msg) use ($callback, $publishRetry, $publishFailed) {
                                $callback($msg, $publishRetry, $publishFailed);
                            }
                        );
                        $foundQueue = true;
                        // 重置连接尝试计数器
                        $connectionAttempts = 0;
                    } catch (\Exception $e) {
                        // 重试
                        $connectionAttempts++;
                        if ($connectionAttempts > 10) {
                            throw $e;
                        }
                        usleep(1000000);
                        continue;
                    }
                } catch (\Exception $e) {
                    // 连接失败，处理异常，根据异常类型再重试或者退出
                    $connectionAttempts ++;
                    if ($connectionAttempts > 10) {
                        throw $e;
                    }
                    usleep(1000000);
                    continue;
                }

                if ($foundQueue) {
                    try {
                        while (count($this->channel->callbacks)) {
                            if ($shouldExitCallback()) {
                                return;
                            }
                            $connectionException = null;
                            try {
                                $this->channel->wait(null, false, 3);
                            } catch (AMQPTimeoutException $e) {
                                $connectionException = $e;
                            } catch (AMQPIOWaitException $e) {
                                $connectionException = $e;
                            }
                            if ($connectionException !== null) {
                                $errMsg = $connectionException->getMessage();
                                $connectionException = null;
                                throw new \Exception($errMsg, 9999);
                            }
                        }
                    } catch(\Exception $e) {
                        if ($e->getCode() == 9999) {
                            sleep(2);
                            $foundQueue = false;
                        } else {
                            throw $e;
                        }
                    }
                }
            }
        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 重试失败的消息
     *
     * 注意： 该方法会堵塞执行
     *
     * @param string   $queueName
     * @param string   $routingKey
     * @param \Closure $callback 回调函数，可以为空，返回true则重新发布，false则丢弃
     * @throws \ErrorException
     */
    public function retryFailed(string $queueName, string $routingKey, $callback = null)
    {
        $this->declareConsumeQueue($queueName, $routingKey);
        $failedQueueName = $this->declareFailedQueue($queueName, $routingKey);

        $this->channel->basic_consume(
            $failedQueueName,
            '',
            false,
            false,
            false,
            false,
            function (AMQPMessage $msg) use ($queueName, $callback) {
                if (is_null($callback) || $callback($msg)) {
                    // 重置header中的x-death属性
                    $msg->set('application_headers', new AMQPTable([
                        'x-orig-routing-key' => $this->getOrigRoutingKey($msg),
                    ]));
                    myEcho("retryFailed-》x-orig-routing-key： " . $this->getOrigRoutingKey($msg));
                    $this->channel->basic_publish(
                        $msg,
                        $this->exchangeTopic(),
                        $queueName
                    );
                }

                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
            }
        );
        while (count($this->channel->callbacks)) {
            try {
                $this->channel->wait(null, false, 3);
            } catch (AMQPTimeoutException $e) {
                return;
            } catch (AMQPIOWaitException $e) {
            }
        }
    }

    /**
     * 声明重试队列
     *
     * @param string $queueName
     * @param string $routingKey
     *
     * @return string
     */
    private function declareRetryQueue(string $queueName, string $routingKey): string
    {
        $retryQueueName = $this->getRetryQueueName($queueName);
        $this->channel->queue_declare(
            $retryQueueName,
            false,
            true,
            false,
            false,
            false,
            new AMQPTable([
                'x-dead-letter-exchange'    => $this->exchangeTopic(),
                'x-dead-letter-routing-key' => $queueName,
                'x-message-ttl'             => RABBITMQ_DEADLETTERTTL,
            ])
        );
        $this->channel->queue_bind($retryQueueName, $this->exchangeRetryTopic(), $queueName);
        //$this->channel->queue_bind($retryQueueName, $this->exchangeRetryTopic(), $routingKey);

        return $retryQueueName;
    }

    /**
     * 声明消费队列
     *
     * @param string $queueName
     * @param string $routingKey
     * @param bool   $isDelayed
     *
     * @return string
     */
    private function declareConsumeQueue(string $queueName, string $routingKey, bool $isDelayed = false): string
    {
        if ($isDelayed) {
            $arguments = new AMQPTable(array(
                "x-dead-letter-exchange" => "delayed"
            ));
        } else {
            $arguments = [];
        }
        $this->channel->queue_declare($queueName, false, true, false, false, false, $arguments);
        $this->channel->queue_bind($queueName, $this->exchangeTopic(), $routingKey);
        $this->channel->queue_bind($queueName, $this->exchangeTopic(), $queueName);
        return $queueName;
    }

    /**
     * 声明消费失败队列
     *
     * @param string $queueName
     * @param string $routingKey
     *
     * @return string
     */
    private function declareFailedQueue(string $queueName, string $routingKey): string
    {
        $failedQueueName = $this->getFailedQueueName($queueName);
        $this->channel->queue_declare($failedQueueName, false, true, false, false, false);
        $this->channel->queue_bind($failedQueueName, $this->exchangeFailedTopic(), $queueName);
        //$this->channel->queue_bind($failedQueueName, $this->exchangeFailedTopic(), $routingKey);

        return $failedQueueName;
    }

    /**
     * 声明创建队列绑定路由
     *
     * @param $queueName
     * @param $routingKey
     * @param $isDelayed
     */
    public function declareQueue($queueName, $routingKey, $isDelayed = false)
    {
        $this->declareRetryQueue($queueName, $routingKey);
        $this->declareConsumeQueue($queueName, $routingKey, $isDelayed);
        $this->declareFailedQueue($queueName, $routingKey);
    }

    public function getQueueMessage($queueName, $routingKey, $limit = 100, $type = 'failed', $isDelayed = false)
    {
        static $messageList;
        static $count = 1;
        if ($type == 'failed') {
            $queueName = $this->declareFailedQueue($queueName, $routingKey);
        } elseif ($type == 'retry') {
            $queueName = $this->declareRetryQueue($queueName, $routingKey);
        } else {
            $queueName = $this->declareConsumeQueue($queueName, $routingKey, $isDelayed);
        }

        while ($count <= $limit) {
            $message = $this->channel->basic_get($queueName);
            if ($message && $message instanceof \PhpAmqpLib\Message\AMQPMessage) {
                $this->channel->basic_nack($message->delivery_info['delivery_tag'], false, true);
                $messageList[] = json_decode($message->body, true)['body'] ?? [];
                if ($count >= $limit) {
                    $this->channel->basic_cancel($message->delivery_info['consumer_tag']);
                }
            } else {
                break;
            }
            $count ++;
        }

        /*
        $callback = function ($message) use ($limit, &$count, &$messageList) {
            $message->delivery_info['channel']->basic_nack($message->delivery_info['delivery_tag'], false, true);
            $messageList[] = json_decode($message->body, true)['body'] ?? [];
            $count ++;
            if ($count >= $limit) {
                $message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']);
            }
        };
        $this->channel->basic_consume(
            $queueName,
            '',
            false,
            false,
            false,
            false,
            $callback
        );
        while ($this->channel->is_consuming()) {
            try {
                $this->channel->wait(null, false, 3);
            } catch (AMQPTimeoutException $e) {
            } catch (AMQPIOWaitException $e) {
            }
        }
        */
        return $messageList;
    }
}
