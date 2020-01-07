<?php

namespace RabbitMqHelper\RabbitMQRetry;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPRuntimeException;

class Publisher extends PubSub implements Publish
{
    /**
     * Publish a message to mq
     *
     * @param array|\Serializable $message
     * @param string              $routingKey
     * @param bool                $exception 是否抛出异常
     * @param integer             $delay 消息延迟 单位秒
     *
     * @return bool|AMQPMessage
     * @throws \Exception
     */
    public function publish($message, string $routingKey, $exception = true, $delay = 0)
    {
        if (is_array($message)) {
            $message = new Message($message);
        }

        if (!$message instanceof \Serializable) {
            throw new \InvalidArgumentException('message必须实现Serializable接口');
        }

        $properties['delivery_mode'] = AMQPMessage::DELIVERY_MODE_PERSISTENT;
        $msg = new AMQPMessage($message->serialize(), $properties);
        if (is_numeric($delay) && $delay > 0) {
            $headers = new AMQPTable(array("x-delay" => $delay * 1000));
            $msg->set('application_headers', $headers);
        }

        $errMsg = '';
        try {
            $exchange = $this->exchangeTopic();
            $this->channel->basic_publish($msg, $exchange, $routingKey);
        } catch (AMQPConnectionClosedException $e) {
            $errMsg = $e->getMessage();
        } catch (AMQPRuntimeException $e) {
            $errMsg = $e->getMessage();
        } catch (\Exception $e) {
            $errMsg = $e->getMessage();
        } catch (\Error $e) {
            $errMsg = $e->getMessage();
        } catch (\Throwable $e) {
            $errMsg = $e->getMessage();
        }

        if (!empty($errMsg)) {
            if ($exception) {
                throw new \Exception('[basic.publish] Exception：' . $errMsg);
            } else {
                return false;
            }
        }

        return $msg;
    }
}
