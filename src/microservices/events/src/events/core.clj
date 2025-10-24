(ns events.core
  (:require
   [events.kafka :as kafka]
   [events.models :as models]
   [malli.core :as m]
   [clojure.tools.logging :as log]

   [cheshire.core :as json]
   [compojure.core :refer [defroutes context GET POST]]
   [compojure.route :refer [not-found]]
   [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
   [ring.middleware.json :refer [wrap-json-body]]
   [compojure.middleware :refer [wrap-canonical-redirect]]
   [org.httpkit.server :as http.server])
  (:gen-class))

(defn bad-request [msg data]
  {:status 400
   :body {:error msg
          :payload data}})

(defn get-env [env-var & [default]]
  (let [env (System/getenv)]
    (get env env-var default)))

(defn get-kafka-brokers []
  (get-env "KAFKA_BROKERS" "localhost:60402"))

(defroutes handler
  (context "/api/events" []
    (GET "/health" []
      {:status 200
       :body {:status true}})

    (POST "/movie" request
      (do
        (log/info "POST /api/events/movie has been requested")

        (let [event (:body request)]
          (if-not (m/validate models/MovieEvent event)
            (bad-request "Incorrect MovieEvent data provided" event)

            (let [topic "movie-events"
                  producer (kafka/create-producer (get-kafka-brokers))
                  consumer (kafka/create-consumer (get-kafka-brokers) topic)]
              (try
                (log/info "Send event to kafka" event)
                (kafka/send-event! producer topic event)

                (Thread/sleep 300)

                (let [msg (kafka/consume-once consumer 2000)]
                  (log/info "Consume event from kafka" msg)
                  (if msg
                    {:status 201
                     :body {:status "success"
                            :partition (:partition msg)
                            :offset (:offset msg)
                            :event
                            {:id (:key msg)
                             :type "movie"
                             :timestamp (:timestamp msg)
                             :payload (:value msg)}}}
                    {:status 500
                     :body {:error "timeout"}}))

                (finally
                  (.close producer)
                  (.close consumer))))))))
    (POST "/user" request
      (let [event (:body request)]
        (if-not (m/validate models/UserEvent event)
          (bad-request "Incorrect UserEvent data provided" event)
          (let [topic "user-events"
                producer (kafka/create-producer (get-kafka-brokers))
                consumer (kafka/create-consumer (get-kafka-brokers) topic)]
            (try
              (kafka/send-event! producer topic event)

              (Thread/sleep 300)

              (let [msg (kafka/consume-once consumer 2000)]
                (if msg
                  {:status 201
                   :body {:status "success"
                          :partition (:partition msg)
                          :offset (:offset msg)
                          :event
                          {:id (:key msg)
                           :type "movie"
                           :timestamp (:timestamp msg)
                           :payload (:value msg)}}}
                  {:status 500
                   :body {:error "timeout"}}))

              (finally
                (.close producer)
                (.close consumer)))))))

    (POST "/payment" request
        (let [event (:body request)]
          (if-not (m/validate models/PaymentEvent event)
            (bad-request "Incorrect PaymentEvent data provided" event)
            (let [topic "payment-events"
                  producer (kafka/create-producer (get-kafka-brokers))
                  consumer (kafka/create-consumer (get-kafka-brokers) topic)]
              (try
                (kafka/send-event! producer topic event)

                (Thread/sleep 300)

                (let [msg (kafka/consume-once consumer 2000)]
                  (if msg
                    {:status 201
                     :body {:status "success"
                            :partition (:partition msg)
                            :offset (:offset msg)
                            :event
                            {:id (:key msg)
                             :type "movie"
                             :timestamp (:timestamp msg)
                             :payload (:value msg)}}}
                    {:status 500
                     :body {:error "timeout"}}))

                (finally
                  (.close producer)
                  (.close consumer))))))))

  (not-found "There is nothing here."))

(def app
  (-> handler
      (wrap-defaults api-defaults)
      (wrap-canonical-redirect)
      (wrap-json-body)
      ((fn wrap-json-response [handler]
         (fn [req]
           (-> (handler req)
               (update :body json/generate-string)
               (assoc-in [:headers "Content-Type"] "application/json")))))))

(defn -main [& args]
  (let [port (Integer/parseInt (get-env "PORT" "8082"))
        server (http.server/run-server #'app {:port port})]
    (log/info "Events server started on port:" port)
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. server))))




(comment
  (do (defonce server (atom nil))

      (defn run-server []
        (reset! server (http.server/run-server #'app {:port 9999})))

      (defn stop-server []
        (@server)))

  (run-server)

  (stop-server)

  (do (require '[clojure.repl.deps :as deps])
      (deps/sync-deps))

  (require '[org.httpkit.client :refer [request]])

  @(request {:url "http://localhost:8082/api/events/movie"
             :method :post
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string {"movie_id" "999"
                                          "title" "Inside out"
                                          "action" "viewed"})})

  @(request {:url "http://localhost:8082/api/events/health"
             :metod :get})

  @(request {:url "http://localhost:9999/api/events/health"
             :method :get
             :body (json/generate-string {:a 1})})

;
  )
