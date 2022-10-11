(ns direct-otel
  (:require
   [clj-http.lite.client :as http]
   [pronto.core :as p]
   [pronto.schema :as s])
  (:import
   (com.google.protobuf ByteString)
   (io.opentelemetry.proto.collector.trace.v1 ExportTraceServiceRequest)
   (java.nio ByteBuffer)))

;; Experiment of doing OpenTelemetry tracing from Clojure more directly, it still
;; uses the POJO classes generated from the protoboffer specification, but from
;; there on does everything without API or SDK support.

;; We use AppsFlyer pronto to get idiomatic access to Protobuffer clasess.

(p/defmapper trace-req [ExportTraceServiceRequest])

(defn unix-nanos-now []
  (let [now (java.time.Instant/now)]
    (+ (.getNano now)
       (* 1e9
          (.getEpochSecond now)))))

(defn rand-trace-id []
  (let [uuid (random-uuid)]
    (.array
     (doto (ByteBuffer/wrap (byte-array 16))
       (.putLong (.getMostSignificantBits uuid))
       (.putLong (.getLeastSignificantBits uuid))))))

(defn rand-span-id []
  (ByteString/copyFrom
   (let [uuid (random-uuid)]
     (.array
      (doto (ByteBuffer/wrap (byte-array 8))
        (.putLong (.getMostSignificantBits uuid)))))))

(defn attr-map [m]
  (map (fn [[k v]]
         {:key (str k)
          :value
          (cond
            ;;:array_value io.opentelemetry.proto.common.v1.ArrayValue,
            (boolean? v)
            {:bool_value v}
            (bytes? v)
            {:bytes_value (ByteString/copyFrom v)}
            (double? v)
            {:double_value v}
            (int? v)
            {:int_value (long v)}
            ;;:kvlist_value io.opentelemetry.proto.common.v1.KeyValueList,
            :else
            {:string_value (str v)}
            )})
       m))

(def ^:dynamic *active-span* nil)

(defn new-span [name attrs]
  (let [trace-id (:trace_id *active-span* (rand-trace-id))
        parent-id (:span_id *active-span*)]
    (cond->
        {:trace_id trace-id
         :span_id  (rand-span-id)
         :name name
         :kind io.opentelemetry.proto.trace.v1.Span$SpanKind/SPAN_KIND_SERVER
         :start_time_unix_nano (unix-nanos-now)
         :attributes (attr-map attrs)}
      parent-id
      (assoc :parent_span_id parent-id))))

(defn finish-span []
  (http/post
   "http://localhost:4318/v1/traces"
   {:headers {"content-type" "application/x-protobuf"}
    :body
    (->
     (p/proto-map trace-req ExportTraceServiceRequest)
     (assoc
       :resource_spans
       [{:resource
         {:attributes (attr-map {"service.name"
                                 "MyService"})}
         :scope_spans
         [{:scope {:attributes []}
           :spans
           [(assoc *active-span* :end_time_unix_nano (unix-nanos-now))]}]}])
     p/proto-map->bytes)}))

(defmacro span {:style/indent 2}
  [name attrs & body]
  `(binding [*active-span* (new-span ~name ~attrs)]
     ~@body
     (finish-span)))

(comment
  (span "/outer" {"http.method" "POST"}
        (Thread/sleep 500)
        (span "/inner" {"http.method" "GET"}
              (Thread/sleep 500)))

  ;; Check schema

  (defn expand [s]
    (clojure.walk/postwalk (fn [o]
                             (if (some #{com.google.protobuf.Message} (ancestors o))
                               (try
                                 (s/schema o)
                                 (catch Exception e
                                   (symbol (str o))))
                               o))
                           s))

  (expand
   (expand
    (expand
     (expand (s/schema ExportTraceServiceRequest))))))

;; docker run --name jaeger \
;; -e COLLECTOR_OTLP_ENABLED=true \
;; -p 6831:6831/udp \
;; -p 6832:6832/udp \
;; -p 5778:5778 \
;; -p 16686:16686 \
;; -p 4317:4317 \
;; -p 4318:4318 \
;; -p 14250:14250 \
;; -p 14268:14268 \
;; -p 14269:14269 \
;; -p 9411:9411 \
;; jaegertracing/all-in-one:latest --log-level=debug
