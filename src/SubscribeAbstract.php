<?php

namespace RabbitMqHelper\RabbitMQRetry;

use RabbitMqHelper\RabbitMQRetry\Helpers;
use RabbitMqHelper\RabbitMQRetry\Subscriber;
use RabbitMqHelper\RabbitMQRetry\RabbitMQ;
use RabbitMqHelper\RabbitMQRetry\SubMessage;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * @Class   SubscribeAbstract
 * @Desc    MQ消息消费处理类
 * @author  Huangbin <huangbin2018@qq.com>
 * @Date    2019/4/10 16:10
 * @package RabbitMqHelper\RabbitMQRetry
 * <code>
 * $subscriber = new SubscribeAbstract($rabbitMQ, ['queueName' => $queueName, 'routingKey' => $routingKey, 'exchangeName' => $exchangeName]);
 * //重发失败消息
 * //$subscriber->retryFailedMessage();
 * //监听消息队列
 * $subscriber->run(function($message) {
 *  //自定义业务处理类...
 *
 * });
 * </code>
 */
class SubscribeAbstract
{
    use Helpers;

    /**
     * @var string
     */
    public $queueName = '';

    /**
     * 消息消费回调函数
     * @var string | null
     */
    protected $subscribeFunc = null;

    /**
     * @var string
     */
    public $routingKey = '';

    /**
     * @var string
     */
    public $exchangeName = '';

    public $writeLogsFlag = false;

    /**
     * 消息延时标识
     * @var bool
     */
    public $isDelayed = false;

    /**
     * @var RabbitMQ|null $rabbitMQ
     */
    public $rabbitMQ = null;

    public function __construct(RabbitMQ $rabbitMQ = null, array $configs = [])
    {
        foreach ($configs as $key => $val) {
            if (isset($this->$key)) {
                $this->$key = $val;
            }
        }
        $this->rabbitMQ     = $rabbitMQ;

        //消费日志记录
        if (!isset($configs['writeLogsFlag'])) {
            $this->writeLogsFlag = (defined('MQ_SUBSCRIBER_EXEC_LOG') && MQ_SUBSCRIBER_EXEC_LOG == 'open') ? true : false;
        }
    }

    /**
     * 消费入口
     * @param $subscribeFunc
     */
    public function run($subscribeFunc)
    {
        $this->subscribeFunc = $subscribeFunc;
        $this->listenAndServe();
    }

    /**
     * 订阅失败的任务
     *
     * 如需按照需要重新订阅或者丢弃失败的消息，覆盖该方法，重新实现自己的逻辑即可，注意的是要返回false，否则
     * 还是会自动重新加入消费队列。
     *
     * 如果返回true，则任务自动重发，如果返回false，则需要自己处理，任务自动丢弃
     *
     * @param SubMessage      $msg
     *
     * @return bool
     */
    public function subscribeFailed($msg): bool
    {
        /*
        echo sprintf(
            '恢复消息 %s, routing_key: %s, body: %s',
            $msg->getMessage()->getID(),
            $msg->getRoutingKey(),
            $msg->getAMQPMessage()->body
        );
        echo "\n------------------------------------\n";
        */
        return true;
    }

    /**
     * 重发失败的消息
     *
     */
    public function retryFailedMessage()
    {
        $callback = function (AMQPMessage $msg) {
            $retry = $this->getRetryCount($msg);

            $subMessage = new SubMessage($msg, $this->getOrigRoutingKey($msg), [
                'retry_count' => $retry, // 重试次数
            ]);

            return $this->subscribeFailed($subMessage);
        };

        $subscriber = new Subscriber($this->getRabbitMQ(), $this->getExchangeName());
        $subscriber->retryFailed(
            $this->getQueueName(),
            $this->getRoutingKey(),
            $callback
        );
    }

    /**
     * 启动消息消费监听
     */
    protected function listenAndServe()
    {
        $callback = function (AMQPMessage $msg, $publishRetry, $publishFailed) {
            $retry = $this->getRetryCount($msg);

            try {
                $subMessage = new SubMessage($msg, $this->getOrigRoutingKey($msg), [
                    'retry_count' => $retry, // 重试次数
                ]);

                if ($this->writeLogsFlag) {
                    ob_start();
                    $time_begin = microtime(true);
                }

                $subscribeAsk = $this->subscribe($subMessage);

                if ($this->writeLogsFlag) {
                    $time_end = microtime(true);
                    $output = ob_get_clean();
                    $run_time = $time = round(($time_end - $time_begin), 2);
                    echo $output;
                    $this->writeExecutionLog(
                        $subMessage,
                        strtolower($subscribeAsk['ask']) == 'success' ? 1 : 0,
                        $run_time,
                        $subscribeAsk['message'] ? ($subscribeAsk['message']
                        .PHP_EOL.$output) : $output
                    );
                }

                if (strtolower($subscribeAsk['ask']) != 'success') {
                    throw new \Exception('Subscribe Exception: ' . $subscribeAsk['message']);
                }

                // 发送确认消息
                $msg->delivery_info['channel']->basic_ack(
                    $msg->delivery_info['delivery_tag']
                );
            } catch (\Exception $ex) {
                if ($retry > RABBITMQ_RETRYCOUNT) {
                    // 超过最大重试次数，消息无法处理
                    $publishFailed($msg, '--- wait retry --- ' . $ex->getMessage());
                    return;
                }

                // 消息处理失败，稍后重试
                $publishRetry($msg, $ex->getMessage());
            }
        };
        $isDelayed = $this->isDelayed;
        $subscriber = new Subscriber($this->getRabbitMQ(), $this->getExchangeName(), $isDelayed);
        $subscriber->consume(
            $this->getQueueName(),
            $this->getRoutingKey(),
            $callback,
            null,
            $isDelayed
        );
    }

    /**
     * 获取消息重试次数
     *
     * @param AMQPMessage $msg
     *
     * @return int
     */
    protected function getRetryCount(AMQPMessage $msg): int
    {
        $retry = 0;
        if ($msg->has('application_headers')) {
            $headers = $msg->get('application_headers')->getNativeData();
            if (isset($headers['x-death'][0]['count'])) {
                $retry = $headers['x-death'][0]['count'];
            }
        }

        return (int)$retry;
    }

    /**
     * 订阅消息处理
     *
     * @param \RabbitMqHelper\RabbitMQRetry\SubMessage $msg
     *
     * @return array ['ask' => 'success', 'message' => 'error message'] ask 处理成功返回 success (返回 success 后将会对消息进行处理确认)，失败返回 failure
     */
    public function subscribe(SubMessage $msg): array
    {
        $subscribeCallback = $this->subscribeFunc;
        $result = $subscribeCallback($msg);
        return $result;
    }

    /**
     * 获取监听的路由
     *
     * @return string
     */
    protected function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    /**
     * 获取监听的队列名称
     *
     * @return string
     */
    protected function getQueueName(): string
    {
        return $this->queueName;
    }

    /**
     * 创建RabbitMQ连接
     *
     * @return \RabbitMqHelper\RabbitMQRetry\RabbitMQ
     */
    protected function getRabbitMQ(): RabbitMQ
    {
        return $this->rabbitMQ;
    }

    /**
     * 返回Exchange name
     *
     * @return string
     */
    protected function getExchangeName(): string
    {
        return $this->exchangeName;
    }

    /**
     * 声明创建队列绑定路由
     * @param string $queueName
     * @param string $routingKey
     * @param string $exchangeName
     * @param bool   $isDelayed
     */
    public function declareQueue($queueName = '', $routingKey = '', $exchangeName = '', $isDelayed = false)
    {
        $subscriber = new Subscriber($this->getRabbitMQ(), $exchangeName ? $exchangeName : $this->getExchangeName(), $isDelayed);
        $subscriber->declareQueue($queueName, $routingKey, $isDelayed);
    }

    public function writeExecutionLog($subMessage, $ask = 0, $runTime = 0, $output = '')
    {
        try {
            if ($subMessage instanceof SubMessage) {
                $routingKey = $subMessage->getRoutingKey();
                $messageJson = $subMessage->getMessage()->serialize();
            } else {
                $routingKey = $messageJson = '';
            }
            global $adminZtId;
            $clsLog = new \WMS\Log();
            $clsLog->setCollection(\WMS\Defines::$MongoCollection['mq_subscriber_execution_log']);
            $clsLog->addLog([
                'zt_id' => $adminZtId ?? 0,
                'exchange' => $this->getExchangeName(),
                'queue' => $this->getQueueName(),
                'routing' => $routingKey,
                'message' => $messageJson,
                'execution_time' => $runTime,
                'execution_output' => $output,
                'execution_ask' => $ask ?? 0,
                'memory_usage' => $this->getMemoryUsage(),
                'log_add_time' => date('Y-m-d H:i:s'),
            ]);
        } catch (\Exception $e) {
        } catch (\Error $e) {
        } catch (\Throwable $e) {
        }
    }

    public function getMemoryUsage($size = false)
    {
        $size = $size ?: memory_get_usage(true);
        $unit = array('b', 'kb', 'mb', 'gb', 'tb', 'pb');
        $exp = floor(log($size, 1024));
        return round($size / pow(1024, $exp), 2) . ' ' . $unit[$exp];
    }

    public function getQueueMessage($queueName = '', $routingKey = '', $exchangeName = '', $limit = 10, $type = 'failed')
    {
        $subscriber = new Subscriber($this->getRabbitMQ(), $exchangeName ? $exchangeName : $this->getExchangeName(), $this->isDelayed);
        return $subscriber->getQueueMessage($queueName, $routingKey, $limit, $type, $this->isDelayed);
    }
}
