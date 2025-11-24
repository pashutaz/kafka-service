<?php

namespace App\Infrastructure\Service;

use RdKafka\Conf;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;

class KafkaClient
{
    private Conf $config;
    private KafkaConsumer $consumer;
    private Producer $producer;
    protected bool $kafkaDebug = false;
    protected array $kafkaBrokers = [];
    protected int $kafkaFlush = 10000;

    public function __construct()
    {
        $this->config = self::configure();
        $this->consumer = new KafkaConsumer($this->config);
        $this->producer = new Producer($this->config);
    }


    public static function listen(string $event, callable $closure)
    {
        $conf = self::configure();
        $conf->set('group.id', 'delivery_service_group');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(["event.$event"]);

        echo 'Waiting for messages...', PHP_EOL;
        while (true) {
            $message = $consumer->consume(1000); // Poll for 1 second

            if ($closure($message)) {
                $consumer->commit($message);
            }

            sleep(4);
        }
    }

    public static function configure(): Conf
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', env('KAFKA_BROKERS', 'localhost:9092')); // Явно указываем брокер
        //$conf->set('group.id', 'kafka-client');

        if (env('KAFKA_DEBUG')) {
            $conf->set('log_level', (string) LOG_DEBUG);
            $conf->set('debug', 'msg');
        }

        return $conf;
    }

    public static function produce(string $event, array $data, array $headers = null, string $key = null): void
    {
        $conf = self::configure();
        $producer = new Producer($conf);

        $topic = $producer->newTopic("event.$event");
        $topic->producev(RD_KAFKA_PARTITION_UA, 0, json_encode($data), $key, $headers);

        // Ожидаем доставки сообщений
        $producer->flush(env('KAFKA_FLUSH', 10000)); // Таймаут в ms
    }

    /**
     * @throws Exception
     */
    public static function consume(string $event): Message
    {
        $conf = self::configure();

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'earliest': start from the beginning
        $conf->set('auto.offset.reset', 'earliest');

        // Emit EOF event when reaching the end of a partition
        $conf->set('enable.partition.eof', 'true');

        $consumer = new KafkaConsumer($conf);
        // Subscribe to topic
        $consumer->subscribe(["event.$event"]);

        return $consumer->consume(env('KAFKA_FLUSH', 1000));
    }


    /**
     * @throws Exception
     */
    public function getEvent(string $event): Message
    {
        $this->consumer->subscribe(["event.$event"]);

        return $this->consumer->consume(env('KAFKA_FLUSH'));
    }

    public function postEvent(string $event, array $data): int
    {
        $topic = $this->producer->newTopic("event.$event");
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($data));

        return $this->producer->flush(env('KAFKA_FLUSH'));
    }
}
