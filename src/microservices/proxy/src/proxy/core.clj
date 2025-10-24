(ns proxy.core
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [clojure.walk :refer [stringify-keys]]
   [org.httpkit.client :as http.client]
   [org.httpkit.server :as http.server])
  (:gen-class))

(defn lottery [prob]
  (<= (rand-int 101) prob))

(defn get-env [env-var & [default]]
  (let [env (System/getenv)]
    (get env env-var default)))

(defn proxy-pass [req url]
  (try
    (let [target-url (str url (:uri req))
          request {:url target-url
                   :method (:request-method req)
                   :headers (:headers req)
                   :query-params (:query-params req)
                   :body (:body req)}
          response (do
                     (log/info "Making request to" target-url)
                     (log/debug "Request details:" request)
                     @(http.client/request request))]
      (log/info "Got response from" target-url response)
      (-> response
          (dissoc :opts)
          (update :headers (fn [headers] (dissoc headers :transfer-encoding)))
          (update :headers stringify-keys)))

    (catch Exception _
      {:status 502
       :body "Proxy error"})))

(defn get-target-url [req]
  (let [monolith-url (get-env "MONOLITH_URL" "http://localhost:8080")
        movies-service-url (get-env "MOVIES_SERVICE_URL" "http://localhost:8081")
        do-migration? (parse-boolean (get-env "GRADUAL_MIGRATION" "false"))
        migration-percent (if do-migration?
                            (Integer/parseInt (get-env "MOVIES_MIGRATION_PERCENT" "0"))
                            0)
        to-redirect? (lottery migration-percent)]
    (cond
      (not to-redirect?)
      monolith-url

      (str/starts-with? (:uri req) "/api/movies")
      movies-service-url

      :else
      monolith-url)))

(defn app [req]
  (if (= (:uri req) "/health")
    {:status 200
     :body "ok"}
    (proxy-pass req (get-target-url req))))

(defn -main [& args]
  (let [port (Integer/parseInt (get-env "PORT" "8000"))
        server (http.server/run-server #'app {:port port})]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. server))))

(comment
  (do (defonce server (atom nil))

      (defn run-server []
        (reset! server (http.server/run-server #'app {:port 9998})))

      (defn stop-server []
        (@server)))


  (run-server)

  (stop-server)

  (require '[clojure.repl.deps :as deps])
  (deps/sync-deps)

;
  )
