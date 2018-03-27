(ns d2q.api
  "Demand-driven querying à la GraphQL / Datomic Pull"
  (:require [clojure.walk]
            [manifold.deferred :as mfd]
            [d2q.impl.utils :as impl.utils]
            [d2q.impl]))

;; TODO improvements (Val, 15 Mar 2018)
;; 1. √ Remove specific authorization model - replace by a query-level resolution step
;; 2. Subquery preview in resolvers? May require to change the format of resolvers.
;; 3. Partial errors => resolvers return a map with {:data :errors} key
;; 4. Maybe skip the normalization of query - should be done by the caller
;; 5. Mutual recursion (requires to change the query format)
;; 6. Source mapping - keep track of (reversed) query-path and data-path
;; 7. Maybe faster processing via records ?

;; IMPROVEMENT maybe we can let the client express than some field is required etc. (Val, 30 May 2017)
(defn normalize-query-field
  [f]
  (as-> f f
    (if (map? f)
      f
      {:d2q.fcall/field f})

    (update f
      :d2q.fcall/key
      #(or % (:d2q.fcall/field f)))

    (if-let [nested (:d2q.fcall/nested f)]
      (assoc f :d2q.fcall/nested (into [] (map normalize-query-field) nested))
      f)))

(defn normalize-query
  [pull-spec]
  (into [] (map normalize-query-field) pull-spec))

(defn query-engine
  "Compiles tabular field resolvers + fields specs + entity-transformation fn to a function for computing demand-driven queries.
  The returned function has signature [qctx, q, obj] -> Deferred<query-result>"
  [tabular-resolvers
   fields
   transform-entities-fn                                    ;; TODO find a better name for this step (Val, 17 Mar 2018)
   ]
  (let [eng (d2q.impl/engine tabular-resolvers fields transform-entities-fn)]
    (fn query [qctx q obj]
      (let [query (normalize-query q)]
        (d2q.impl/query eng qctx query obj)))))

;; ------------------------------------------------------------------------------
;; Generalization of the Fields-resolver model

(defn tabular-resolver-from-field-resolvers
  [tr-name fields]
  {:d2q.resolver/name tr-name
   :d2q.resolver/compute
   (let [frs-by-field-name (impl.utils/index-and-map-by
                             :d2q.field/name
                             :bs.d2q.field/compute
                             fields)]
     (fn resolve-table [qctx f+args o+is]
       (mfd/future
         (let [fs (->> f+args
                    (mapv (fn [[k field-name args]]
                            (let [compute-fn (or
                                               (frs-by-field-name field-name)
                                               (throw (ex-info
                                                        (str "No field with name " (pr-str field-name)
                                                          " is supported by tabular resolver " (pr-str tr-name))
                                                        {:d2q.fcall/field field-name
                                                         :d2q.fcall/args args
                                                         :d2q.resolver/name tr-name})))]
                              [k compute-fn args field-name]))))]
           (into []
             (remove nil?)
             (for [[obj i] o+is
                   [k compute-fn args field-name] fs]
               (let [v (try
                         ;; TODO use deps argument ? (Val, 16 Nov 2017)
                         (compute-fn qctx obj nil args)
                         (catch Throwable err
                           (throw
                             (ex-info
                               (str
                                 "Field Resolver for key " (pr-str (:d2q.fcall/key k))
                                 " failed with " (pr-str (type err))
                                 " : " (.getMessage err))
                               (merge
                                 {:q-field {:d2q.fcall/field field-name
                                            :d2q.fcall/key k
                                            :d2q.fcall/args args}
                                  :d2q.error/type :d2q.error.type/field-resolver-failed-to-compute}
                                 (ex-data err))
                               err))))]
                 (when (some? v)
                   [i k v]))))))))})