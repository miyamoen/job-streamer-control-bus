(ns job-streamer.control-bus.config
  (:require [environ.core :refer [env]]
            [job-streamer.control-bus.model :as model]))

(def defaults
  {:http {:port 45102}
   :app {:same-origin {:access-control-allow-origin "http://localhost:3000"}}
   :discoverer {:ws-port 45102}
   :scheduler  {:host "localhost"
                :port 45102}
   :migration {:dbschema model/dbschema}
   :auth {:access-control-allow-origin "http://localhost:3000"}
   :token {:token-type "Token"}
   :datomic {:uri "datomic:free://localhost:4334/job-streamer"}})

(def environ
  (let [port (some-> env :control-bus-port Integer.)
        datomic-uri (:datomic-uri env)
        access-control-allow-origin (some-> env :access-control-allow-origin)]
  {:http {:port port}
   :app {:same-origin {:access-control-allow-origin access-control-allow-origin}
         :oauth2 {:active? (:oauth2 env)}}
   :discoverer {:ws-port port}
   :scheduler  {:host "localhost"
                :port port}
   :auth {:access-control-allow-origin access-control-allow-origin}
   :token {:introspection-url (:introspection-url env)
           :token-type (:token-type env)}
   :datomic {:uri datomic-uri}}))

