(ns events.kafka
  (:require
   [cheshire.core :as json])
  (:import
   [java.time Duration Instant]
   [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
   [org.apache.kafka.clients.consumer KafkaConsumer]
   [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(defn create-producer [bootstrap-servers]
  (KafkaProducer. {"bootstrap.servers" bootstrap-servers
                   "key.serializer" StringSerializer
                   "value.serializer" StringSerializer
                   "acks" "all"}))

(defn create-consumer [bootstrap-servers topic]
  (doto (KafkaConsumer. {"bootstrap.servers" bootstrap-servers
                         "group.id" (str "temp-group-" (System/currentTimeMillis))
                         "key.deserializer" StringDeserializer
                         "value.deserializer" StringDeserializer
                         "auto.offset.reset" "earliest"})
    (.subscribe [topic])))

(defn send-event!
  [^KafkaProducer producer ^String topic event]
  (let [record (ProducerRecord. topic (json/generate-string event))]
    (.send producer record)
    (.flush producer)))

(defn consume-once [^KafkaConsumer c timeout-ms]
  (let [records (.poll c (Duration/ofMillis timeout-ms))]
    (when (seq records)
      (let [r (first records)]
        {:key (.key r)
         :value (.value r)
         :timestamp (str (Instant/ofEpochMilli (.timestamp r)))
         :partition (.partition r)
         :offset (.offset r)}))))


(comment
  (def producer (create-producer "localhost:60402"))

  (def consumer (create-consumer "localhost:60402" "movie-events"))

  (send-event! producer "movie-events" {"movie_id" "2"
                                        "title" "Inside Out"
                                        "action" "viewed"})

  (def msg
    (consume-once consumer 2000)


    )

  msg

  (do
    (.close producer)
    (.close consumer))

  ;
  )
