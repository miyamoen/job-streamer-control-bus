(ns job-streamer.control-bus.component.token
  "Provides a token for authorization."
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.cache :as cache]
            [liberator.core :as liberator]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            (job-streamer.control-bus.component
             [datomic :as d]))
  (:import [java.util UUID]))

(defprotocol ITokenProvider
  (new-token      [this user])
  (auth-by        [this token])
  (check-token    [this token])
  (get-token-type [this]))

(defn token-resource [{:keys [datomic token]}]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:post]
    :malformed? (fn [ctx]
                  (if-let [identity (get-in ctx [:request :identity])]
                    [false {::identity identity}]
                    (if-let [code (get-in ctx [:request :params :code])]
                      (if-let [identity (d/query datomic
                                                 '{:find [(pull ?s [:user/name :user/email]) .]
                                                   :in [$ ?token]
                                                   :where [[?s :user/token ?token]]} code)]
                        [false {::identity identity}]
                        {:message "code is invalid."})
                      {:message "code is required."})))

    :post! (fn [{identity ::identity}]
             (let [access-token (new-token token identity)]
               {::post-response (merge identity
                                       {:token-type "bearer"
                                        :access-token access-token})}))
    :handle-created (fn [ctx]
                      (::post-response ctx))))

(defrecord TokenProvider [disposable? introspection-url token-type]
  component/Lifecycle

  (start [component]
         (if (:token-cache component)
           component
           (let [token-cache (atom (cache/ttl-cache-factory {} :ttl (* 30 60 1000)))]
             (assoc component :token-cache token-cache))))

  (stop [component]
        (if (:disposable? config)
          (dissoc component :token-cache)
          component))

  ITokenProvider
  (new-token [component user]
             (let [token (java.util.UUID/randomUUID)]
               (swap! (:token-cache component) assoc token user)
               token))

  (auth-by [component token]
           (let [uuid-token (condp instance? token
                              String (UUID/fromString token)
                              UUID   token)]
             (cache/lookup @(:token-cache component) uuid-token)))

  (defn check-token [component access-token]
    (let [res (client/get (str introspection-url
                               "?token="
                               access-token
                               "&token_hint=hint")
                          {:headers {"Content-type" "application/x-www-form-urlencoded"}
                          :throw-exceptions false})]
      (when (== 200 (:status res))
        (let [{:keys [active client_id username scope token_type nbf sub aud iss exp iat jti] :as res}
              (json/read-str (:body res) :key-fn keyword)]
          active))))

  (defn get-token-type [component]
    token-type))

(defn token-provider-component [options]
  (map->TokenProvider options))
