<?php

namespace RabbitMqHelper\RabbitMQRetry;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;

/**
 * Class RabbitMQ
 *
 * @package RabbitMqHelper\RabbitMQRetry
 */
class RabbitMQ
{
    /**
     * @var string
     */
    private $host = '127.0.0.1';

    /**
     * @var int
     */
    private $port = 5672;

    /**
     * @var string
     */
    private $user = 'guest';

    /**
     * @var string
     */
    private $password = 'guest';

    /**
     * @var string
     */
    private $vhost = '/';

    /**
     * @var AMQPStreamConnection
     */
    private $connection;

    /**
     * @var AMQPChannel[]
     */
    private $channels = [];

    const WAIT_BEFORE_RECONNECT_US = 1000000;

    public function __construct(array $configs = [])
    {
        foreach ($configs as $key => $val) {
            if (isset($this->$key)) {
                $this->$key = $val;
            }
        }

        while (true) {
            $connectionException = null;
            $connectionAttempts = 0;
            try {
                try {
                    $this->doConnect();
                } catch (AMQPRuntimeException $e) {
                    $this->cleanupConnection();
                    usleep(self::WAIT_BEFORE_RECONNECT_US);
                    $connectionException = $e;
                } catch (\RuntimeException $e) {
                    $this->cleanupConnection();
                    usleep(self::WAIT_BEFORE_RECONNECT_US);
                    $connectionException = $e;
                } catch (\ErrorException $e) {
                    $this->cleanupConnection();
                    usleep(self::WAIT_BEFORE_RECONNECT_US);
                    $connectionException = $e;
                }

                // 有异常，则重连
                if ($connectionException !== null) {
                    $msg = $connectionException->getMessage();
                    $code = $connectionException->getCode();
                    $connectionException = null;
                    throw new \Exception($msg, $code);
                }
                break;
            } catch (\Exception $e) {
                if ($e->getCode() == 9999) {
                    $connectionAttempts ++;
                }
                if ($connectionAttempts > 10 || $e->getCode() != 9999) {
                    throw $e;
                } elseif ($connectionAttempts <= 10 && $e->getCode() == 9999) {
                    // 连接重试
                    continue;
                } else {
                    throw $e;
                }
            }
        }
    }

    /**
     * 创建连接
     * @author Huangbin <huangbin2018@qq.com>
     */
    public function doConnect()
    {
        // 设置读写超时时间以及心跳机制，读写超时是心跳的 2 倍以上
        $this->connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost,
            false,'AMQPLAIN',null,'en_US',60,60,null,true,30
        );
        return $this;
    }

    /**
     * @desc 清除连接
     * @author Huangbin <huangbin2018@qq.com>
     */
    public function cleanupConnection()
    {
        foreach ($this->channels as $channel) {
            if (!empty($channel)) {
                $channel->close();
            }
        }

        if (!empty($this->connection)) {
            $this->connection->close();
        }
        return $this;
    }

    /**
     * Create a new channel
     *
     * @param string $channel_id
     *
     * @return AMQPChannel
     */
    public function channel($channel_id = null)
    {
        if (isset($this->channels[$channel_id]) && $this->channels[$channel_id]->is_open()) {
            return $this->channels[$channel_id];
        }

        return $this->channels[$channel_id] = $this->connection->channel($channel_id);
    }

    /**
     * Get RabbitMQ Connection
     *
     * @return AMQPStreamConnection
     */
    public function connection()
    {
        return $this->connection;
    }

    /**
     * RabbitMQ destructor
     */
    public function __destruct()
    {
        $this->cleanupConnection();
    }
}
