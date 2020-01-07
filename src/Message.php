<?php

namespace RabbitMqHelper\RabbitMQRetry;

/**
 * Class Message
 *
 * @package RabbitMqHelper\RabbitMQRetry
 */
class Message implements \Serializable
{
    private $message = [];

    public function __construct(array $message = [], $source = '')
    {
        $this->message = [
            'body' => $message,
            '__id' => uniqid($this->getSelfIp() . '-' . mt_rand(0, 99999) . '-', true),
            '__timestamp' => time(),
            '__source' => $source ? $source : $this->getSelfIp(),
        ];
    }

    /**
     * Get Message
     *
     * @return array
     */
    public function getMessage()
    {
        return isset($this->message['body']) ? $this->message['body'] : null;
    }

    /**
     * 获取消息ID
     *
     * @return string
     */
    public function getID()
    {
        return isset($this->message['__id']) ? $this->message['__id'] : null;
    }

    /**
     * String representation of object
     *
     * @link  http://php.net/manual/en/serializable.serialize.php
     * @return string the string representation of the object or null
     * @since 5.1.0
     */
    public function serialize()
    {
        return json_encode($this->message, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * Constructs the object
     *
     * @link  http://php.net/manual/en/serializable.unserialize.php
     *
     * @param string $serialized <p>
     *                           The string representation of the object.
     *                           </p>
     *
     * @return void
     * @since 5.1.0
     */
    public function unserialize($serialized)
    {
        $this->message = json_decode($serialized, true);
    }

    /**
     * 获取服务器IP
     * @return string
     */
    public function getSelfIp()
    {
        if (isset($_SERVER['HTTP_X_FORWARDED_HOST']) && !empty($_SERVER['HTTP_X_FORWARDED_HOST'])) {
            $host = $_SERVER['HTTP_X_FORWARDED_HOST'];
            $elements = explode(',', $host);
            $host = trim(end($elements));
        } else {
            if (isset($_SERVER['HTTP_HOST']) && !empty($_SERVER['HTTP_HOST'])) {
                $host = $_SERVER['HTTP_HOST'];
            } elseif (isset($_SERVER['SERVER_NAME']) && !empty($_SERVER['SERVER_NAME'])) {
                $host = $_SERVER['SERVER_NAME'];
            } elseif (isset($_SERVER['SERVER_ADDR']) && !empty($_SERVER['SERVER_ADDR'])) {
                $host = $_SERVER['SERVER_ADDR'];
            } else {
                $host = '127.0.0.1';
            }
        }
        return $host ?: '127.0.0.1';
    }

    /**
     * 获取客户端IP
     * @return string
     */
    public function getClientIp()
    {
        $ip = '';
        if (isset($_SERVER['HTTP_X_FORWARDED_FOR'])) {
            $arr = explode(',', $_SERVER['HTTP_X_FORWARDED_FOR']);
            $pos = array_search('unknown', $arr);
            if (false !== $pos) {
                unset($arr[$pos]);
            }
            if (!count($arr)) {
                return '127.0.0.1';
            }
            // 过滤完数据 获取第一个地址
            foreach ($arr as $value) {
                $ip = trim($value);
                break;
            }
        } elseif (isset($_SERVER['X-Real-IP']) && !empty($_SERVER['X-Real-IP'])) {
            $ip = $_SERVER['X-Real-IP'];
        } elseif (isset($_SERVER['REMOTE_ADDR'])) {
            $ip = $_SERVER['REMOTE_ADDR'];
        } elseif (isset($_SERVER['HTTP_CLIENT_IP'])) {
            $ip = $_SERVER['HTTP_CLIENT_IP'];
        }
        return $ip ?: '127.0.0.1';
    }
}
